from datetime import datetime, timezone

from ingestion.data_validator import CustomDataValidator, DataQualityError
from ingestion.exceptions import IngestionError
from ingestion.schema_manager import INGESTED_AT_COLUMN, SchemaManager
from registries.native_ingestions import get_ingestion_handler
from registries.native_publishers import get_publisher_handler
from utils.chunking import chunked

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

            if self.config.destination.mode == "truncate":
                # Reset once, up front - each chunk below only inserts, so a
                # multi-chunk run doesn't wipe out its own earlier chunks.
                self.publisher.reset_table(self.table_name, schema)

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

            id_cols = self.config.loading.id_cols
            batch_size = self.config.loading.batch_size

            extracted_count = 0
            published_count = 0
            quality_summary = {
                "enabled": self.config.data_quality.enabled,
                "total_expectations": 0,
                "passed": 0,
                "failed": 0,
                "results": [],
            }

            # Extracted, validated, and published one batch_size chunk at a
            # time - rather than extracting everything into memory before any
            # of it is validated or written - so a failure partway through a
            # large hist/backfill run doesn't lose chunks that already landed.
            for chunk in chunked(records, batch_size):
                ingested_at = datetime.now(timezone.utc)
                for record in chunk:
                    record[INGESTED_AT_COLUMN] = ingested_at

                if id_cols:
                    self.validate_unique_keys(chunk, id_cols)

                self.schema_manager.validate_records(chunk, schema)

                if self.config.data_quality.enabled:
                    chunk_quality = self.validator.validate(
                        chunk, self.config.data_quality.expectations
                    )
                    quality_summary["total_expectations"] += chunk_quality["total_expectations"]
                    quality_summary["passed"] += chunk_quality["passed"]
                    quality_summary["failed"] += chunk_quality["failed"]
                    quality_summary["results"].extend(chunk_quality["results"])

                publish_summary = self.publisher.publish(
                    table_name=self.table_name,
                    records=chunk,
                    schema=schema,
                    mode=self.config.destination.mode,
                    id_cols=id_cols,
                )

                extracted_count += len(chunk)
                published_count += publish_summary["inserted_or_updated"]

            return {
                "table": self.table_name,
                "extracted": extracted_count,
                "published": published_count,
                "watermark_before_run": str(watermark) if watermark is not None else None,
                "data_quality": quality_summary,
            }
        except IngestionError:
            raise
        except Exception as exc:
            raise IngestionError(f"Ingestion failed for '{self.table_name}': {exc}") from exc
