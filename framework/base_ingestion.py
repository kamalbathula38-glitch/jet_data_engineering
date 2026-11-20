from datetime import datetime, timezone

from framework.data_validator import CustomDataValidator, DataQualityError
from framework.exceptions import FrameworkError
from framework.schema_manager import INGESTED_AT_COLUMN, SchemaManager
from registries.native_ingestions import get_ingestion_handler
from registries.native_publishers import get_publisher_handler

class BaseIngestion:
    def __init__(self, parsed_config: dict):
        self.table_name = parsed_config["table_name"]
        self.config = parsed_config["config"]
        self.schema_path = parsed_config["schema_path"]
        self.schema_manager = SchemaManager()
        self.validator = CustomDataValidator()
        self.ingestion_handler = get_ingestion_handler(self.config.source.ingest_type)
        self.publisher = get_publisher_handler(self.config.destination.publish_type)

    def validate_unique_keys(
        self,
        records: list[dict],
        id_cols: list[str]
    ) -> None:

        seen_keys = set()
        duplicates = set()

        for index, record in enumerate(records):

            key_values = []

            for column in id_cols:
                if column not in record:
                    raise DataQualityError(
                        f"Missing key column '{column}' in record {index}"
                    )

                value = record[column]

                if value is None:
                    raise DataQualityError(
                        f"NULL value found in key column '{column}' "
                        f"at record {index}"
                    )

                key_values.append(value)

            key = tuple(key_values)

            if key in seen_keys:
                duplicates.add(key)
            else:
                seen_keys.add(key)

        if duplicates:
            raise DataQualityError(
                f"Duplicate keys found in ingestion batch: "
                f"{list(duplicates)[:10]}"
            )

    def run(self) -> dict:
        try:
            schema = self.schema_manager.load_schema(self.schema_path)
            self.schema_manager.validate_schema_config(
                schema,
                self.config.loading.id_cols,
                self.config.source.watermark_column,
            )

            self.publisher.ensure_table(
                self.table_name, schema, self.config.loading.id_cols
            )

            watermark = None
            if self.config.source.mode == "incr":
                watermark = self.publisher.get_watermark(
                    self.table_name, self.config.source.watermark_column
                )

            records = self.ingestion_handler.extract(
                mode=self.config.source.mode,
                config=self.config.model_dump(),
                watermark=watermark,
            )

            ingested_at = datetime.now(timezone.utc)
            for record in records:
                record[INGESTED_AT_COLUMN] = ingested_at

            id_cols = self.config.loading.id_cols

            if id_cols:
                self.validate_unique_keys(records, id_cols)

            self.schema_manager.validate_records(records, schema)

            quality_summary = {"enabled": False, "total_expectations": 0}
            if self.config.data_quality.enabled:
                quality_summary = self.validator.validate(
                    records, self.config.data_quality.expectations
                )
                quality_summary["enabled"] = True

            publish_summary = self.publisher.publish(
                table_name=self.table_name,
                records=records,
                schema=schema,
                mode=self.config.destination.mode,
                id_cols=id_cols,
                batch_size=self.config.loading.batch_size
            )

            return {
                "table": self.table_name,
                "extracted": len(records),
                "published": publish_summary["inserted_or_updated"],
                "watermark_before_run": str(watermark) if watermark is not None else None,
                "data_quality": quality_summary,
            }
        except FrameworkError:
            raise
        except Exception as exc:
            raise FrameworkError(f"Ingestion failed for '{self.table_name}': {exc}") from exc
