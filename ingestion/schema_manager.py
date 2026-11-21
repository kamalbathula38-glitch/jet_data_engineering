
from pathlib import Path

import yaml

from ingestion.exceptions import SchemaError


TYPE_MAP = {
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "text": "TEXT",
    "varchar": "TEXT",
    "float": "DOUBLE PRECISION",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "timestamptz": "TIMESTAMPTZ",
}

# Metadata column stamped on every record by BaseIngestion.run(), once per run.
# Excluded from upsert change-detection since it always differs between runs.
INGESTED_AT_COLUMN = "ingested_at"


class SchemaManager:

    def load_schema(self, schema_path: str | Path) -> dict:
        """
        Load and validate the schema YAML definition.
        """
        try:
            with Path(schema_path).open(
                "r", encoding="utf-8"
            ) as file:
                schema = yaml.safe_load(file)

            if (
                not isinstance(schema, dict)
                or not isinstance(schema.get("columns"), list)
            ):
                raise ValueError(
                    "Schema must contain a 'columns' list"
                )

            columns = schema["columns"]

            if not columns:
                raise ValueError(
                    "Schema must define at least one column"
                )

            names = []

            for column in columns:
                if not isinstance(column, dict):
                    raise ValueError(
                        "Each schema column must be a mapping"
                    )

                name = column.get("name")
                data_type = column.get("data_type")

                if not isinstance(name, str) or not name:
                    raise ValueError(
                        "Every column must have a valid name"
                    )

                if name in names:
                    raise ValueError(
                        f"Duplicate column definition: {name}"
                    )

                names.append(name)

                if data_type not in TYPE_MAP:
                    raise ValueError(
                        f"Unsupported data type for '{name}': "
                        f"{data_type}"
                    )

                if not isinstance(column.get("nullable"), bool):
                    raise ValueError(
                        f"Column '{name}' must define "
                        "'nullable' as true or false"
                    )

            columns.append({
                "name": INGESTED_AT_COLUMN,
                "data_type": "timestamptz",
                "nullable": False,
            })

            return schema

        except (OSError, yaml.YAMLError, ValueError) as exc:
            raise SchemaError(
                f"Could not load schema '{schema_path}': {exc}"
            ) from exc

    def validate_schema_config(
        self,
        schema: dict,
        id_cols: list[str] | None = None,
        watermark_column: str | None = None,
    ) -> None:
        """
        Validate configured key and watermark columns
        against the schema definition.
        """
        id_cols = id_cols or []

        columns = {
            column["name"]: column
            for column in schema["columns"]
        }

        for name in id_cols:
            if name not in columns:
                raise SchemaError(
                    f"id_cols column '{name}' "
                    "is missing from the schema"
                )

            if columns[name]["nullable"]:
                raise SchemaError(
                    f"id_cols column '{name}' "
                    "must be non-nullable"
                )

        if watermark_column:
            if watermark_column not in columns:
                raise SchemaError(
                    f"Watermark column '{watermark_column}' "
                    "is missing from the schema"
                )

    def validate_records(
        self,
        records: list[dict],
        schema: dict,
    ) -> None:
        """
        Validate extracted records against the schema.

        Checks:
        - All expected columns exist.
        - No unexpected columns are present.
        - Non-nullable columns contain no NULL values.
        - Values match their declared data types.
        """
        columns = {
            column["name"]: column
            for column in schema["columns"]
        }

        for index, record in enumerate(records):

            if not isinstance(record, dict):
                raise SchemaError(
                    f"Record {index} must be a dictionary"
                )

            unknown = set(record) - set(columns)
            missing = set(columns) - set(record)

            if unknown:
                raise SchemaError(
                    f"Record {index} contains unknown columns: "
                    f"{sorted(unknown)}"
                )

            if missing:
                raise SchemaError(
                    f"Record {index} is missing columns: "
                    f"{sorted(missing)}"
                )

            for name, definition in columns.items():

                value = record[name]

                if value is None:
                    if not definition["nullable"]:
                        raise SchemaError(
                            f"Record {index}: column '{name}' "
                            "cannot be NULL"
                        )
                    continue

                expected = definition["data_type"]

                validators = {
                    "integer": lambda v: (
                        isinstance(v, int)
                        and not isinstance(v, bool)
                    ),
                    "bigint": lambda v: (
                        isinstance(v, int)
                        and not isinstance(v, bool)
                    ),
                    "text": lambda v: isinstance(v, str),
                    "varchar": lambda v: isinstance(v, str),
                    "float": lambda v: (
                        isinstance(v, (int, float))
                        and not isinstance(v, bool)
                    ),
                    "boolean": lambda v: isinstance(v, bool),
                    "date": lambda v: (
                        hasattr(v, "year")
                        and hasattr(v, "month")
                        and hasattr(v, "day")
                    ),
                    "timestamp": lambda v: (
                        hasattr(v, "hour")
                        and hasattr(v, "minute")
                        and hasattr(v, "tzinfo")
                    ),
                    "timestamptz": lambda v: (
                        hasattr(v, "tzinfo")
                        and v.tzinfo is not None
                    ),
                }

                if not validators[expected](value):
                    raise SchemaError(
                        f"Record {index}: column '{name}' "
                        f"expected {expected}, "
                        f"got {type(value).__name__}"
                    )
