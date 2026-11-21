import os

import psycopg
from psycopg import sql

from ingestion.exceptions import PublishingError
from ingestion.schema_manager import INGESTED_AT_COLUMN, TYPE_MAP


class PostgresPublisher:
    def __init__(self, connection_string: str | None = None):
        self.connection_string = connection_string or os.getenv("DATABASE_URL")

        if not self.connection_string:
            raise PublishingError("DATABASE_URL is not set")

    def _connect(self):
        return psycopg.connect(self.connection_string)

    def ensure_table(
        self,
        table_name: str,
        schema: dict,
        id_cols: list[str] | None = None,
    ) -> None:
        """
        Create the destination table if it does not exist.

        If id_cols are provided, create a primary key using those columns.
        Otherwise, create the table without a primary key.
        """
        id_cols = id_cols or []

        try:
            definitions = []

            for column in schema["columns"]:
                definition = sql.SQL("{} {}{}").format(
                    sql.Identifier(column["name"]),
                    sql.SQL(TYPE_MAP[column["data_type"]]),
                    (
                        sql.SQL(" NOT NULL")
                        if not column["nullable"]
                        else sql.SQL("")
                    ),
                )
                definitions.append(definition)

            if id_cols:
                primary_key = sql.SQL("PRIMARY KEY ({})").format(
                    sql.SQL(", ").join(
                        sql.Identifier(name) for name in id_cols
                    )
                )
                definitions.append(primary_key)

            statement = sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} ({})"
            ).format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(definitions),
            )

            with self._connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(statement)

        except Exception as exc:
            raise PublishingError(
                f"Could not create/verify table '{table_name}': {exc}"
            ) from exc

    def get_watermark(
        self,
        table_name: str,
        watermark_column: str,
    ):
        """
        Retrieve the maximum watermark from the destination table.

        Returns None when the table contains no records.
        """
        try:
            statement = sql.SQL(
                "SELECT MAX({}) FROM {}"
            ).format(
                sql.Identifier(watermark_column),
                sql.Identifier(table_name),
            )

            with self._connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(statement)
                    return cursor.fetchone()[0]

        except Exception as exc:
            raise PublishingError(
                f"Could not read watermark from '{table_name}': {exc}"
            ) from exc

    def publish(
        self,
        table_name: str,
        records: list[dict],
        schema: dict,
        mode: str,
        id_cols: list[str] | None = None,
    ) -> dict:
        """
        Dispatch records to upsert or truncate publishing.
        """
        id_cols = id_cols or []

        if mode == "upsert":
            if not id_cols:
                raise PublishingError(
                    "id_cols is required for upsert mode"
                )

            return self.upsert(
                table_name=table_name,
                records=records,
                schema=schema,
                id_cols=id_cols,
            )

        if mode == "truncate":
            if id_cols:
                raise PublishingError(
                    "id_cols must not be provided for truncate mode"
                )

            return self.truncate(
                table_name=table_name,
                records=records,
                schema=schema,
            )

        raise PublishingError(
            f"Unsupported publishing mode: {mode}"
        )

    def upsert(
        self,
        table_name: str,
        records: list[dict],
        schema: dict,
        id_cols: list[str],
    ) -> dict:
        """
        Insert records or update existing records using id_cols
        as the conflict target.
        """
        if not id_cols:
            raise PublishingError(
                "id_cols cannot be empty for upsert"
            )

        if not records:
            return {"inserted_or_updated": 0}

        columns = [
            column["name"] for column in schema["columns"]
        ]

        update_cols = [
            column for column in columns
            if column not in id_cols
        ]

        # Excludes the per-run ingestion timestamp, which always differs from
        # the stored value, so a no-op re-ingestion does not churn the row.
        guard_cols = [
            column for column in update_cols
            if column != INGESTED_AT_COLUMN
        ]

        insert_statement = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({})"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(
                sql.Identifier(column) for column in columns
            ),
            sql.SQL(", ").join(
                sql.Placeholder() for _ in columns
            ),
        )

        if update_cols:
            set_clause = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(
                    sql.Identifier(column),
                    sql.Identifier(column),
                )
                for column in update_cols
            )

            if guard_cols:
                # Only perform the update when at least one real data column
                # actually changed, so re-ingesting unchanged rows is a no-op.
                guard_clause = sql.SQL(" WHERE ({}) IS DISTINCT FROM ({})").format(
                    sql.SQL(", ").join(
                        # Qualified with the table name: Postgres treats a bare
                        # column reference here as ambiguous.
                        sql.Identifier(table_name, column) for column in guard_cols
                    ),
                    sql.SQL(", ").join(
                        sql.SQL("EXCLUDED.{}").format(sql.Identifier(column))
                        for column in guard_cols
                    ),
                )
            else:
                guard_clause = sql.SQL("")

            conflict_action = sql.SQL("DO UPDATE SET {}{}").format(
                set_clause, guard_clause
            )
        else:
            conflict_action = sql.SQL("DO NOTHING")

        statement = (
            insert_statement
            + sql.SQL(" ON CONFLICT ({}) ").format(
                sql.SQL(", ").join(
                    sql.Identifier(name) for name in id_cols
                )
            )
            + conflict_action
        )

        try:
            with self._connect() as conn:
                with conn.cursor() as cursor:
                    params = [
                        tuple(record[column] for column in columns)
                        for record in records
                    ]
                    cursor.executemany(statement, params)

            return {"inserted_or_updated": len(records)}

        except Exception as exc:
            raise PublishingError(
                f"Upsert into '{table_name}' failed: {exc}"
            ) from exc

    def truncate(
        self,
        table_name: str,
        records: list[dict],
        schema: dict,
    ) -> dict:
        """
        Load fresh records into a table already reset by reset_table().
        Called once per chunk - must not itself touch the table's schema, or
        a multi-chunk run would wipe out every earlier chunk's rows.
        """
        if not records:
            return {"inserted": 0}

        try:
            columns = [
                column["name"]
                for column in schema["columns"]
            ]

            insert_statement = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({})"
            ).format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(
                    sql.Identifier(column)
                    for column in columns
                ),
                sql.SQL(", ").join(
                    sql.Placeholder()
                    for _ in columns
                ),
            )

            params = [
                tuple(record[column] for column in columns)
                for record in records
            ]

            with self._connect() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(insert_statement, params)

            return {"inserted": len(records)}

        except Exception as exc:
            raise PublishingError(
                f"Insert into '{table_name}' failed: {exc}"
            ) from exc

    def reset_table(self, table_name: str, schema: dict) -> None:
        """
        Drop and recreate the destination table, without a primary key.
        Called once per run, before any chunk is published - truncate() only
        inserts, so a multi-chunk run doesn't wipe out its own earlier chunks.
        """
        try:
            with self._connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        sql.SQL("DROP TABLE IF EXISTS {}").format(
                            sql.Identifier(table_name)
                        )
                    )

                    definitions = []

                    for column in schema["columns"]:
                        definition = sql.SQL("{} {}{}").format(
                            sql.Identifier(column["name"]),
                            sql.SQL(TYPE_MAP[column["data_type"]]),
                            (
                                sql.SQL(" NOT NULL")
                                if not column["nullable"]
                                else sql.SQL("")
                            ),
                        )
                        definitions.append(definition)

                    create_statement = sql.SQL(
                        "CREATE TABLE {} ({})"
                    ).format(
                        sql.Identifier(table_name),
                        sql.SQL(", ").join(definitions),
                    )

                    cursor.execute(create_statement)

        except Exception as exc:
            raise PublishingError(
                f"Could not reset table '{table_name}': {exc}"
            ) from exc