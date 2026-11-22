from pathlib import Path
import re

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ingestion.exceptions import ConfigurationError


class StrictConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SourceConfig(StrictConfig):
    ingest_type: str
    mode: str
    watermark_column: str | None = None
    backfill_start: int | None = None
    backfill_end: int | None = None


class DestinationConfig(StrictConfig):
    publish_type: str
    mode: str


class SchemaConfig(StrictConfig):
    mode: str = "explicit"
    definition: str | None = None


class LoadingConfig(StrictConfig):
    id_cols: list[str] | None = None
    # None means no chunking: extract, validate, and publish everything in
    # one go.
    batch_size: int | None = None


class DataQualityConfig(StrictConfig):
    enabled: bool = True
    expectations: list[dict] = Field(default_factory=list)


class IngestionConfig(StrictConfig):
    source: SourceConfig
    destination: DestinationConfig
    schema: SchemaConfig
    loading: LoadingConfig
    data_quality: DataQualityConfig = Field(default_factory=DataQualityConfig)


def load_config(
    config_path: str,
    backfill_start: int | None = None,
    backfill_end: int | None = None,
) -> dict:
    path = Path(config_path).resolve()
    try:
        with path.open("r", encoding="utf-8") as file:
            raw = yaml.safe_load(file)

        if not isinstance(raw, dict) or len(raw) != 1:
            raise ValueError("An ingestion config must contain exactly one root key")

        table_name, config_data = next(iter(raw.items()))
        if not isinstance(table_name, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", table_name):
            raise ValueError(f"Invalid target table name: {table_name!r}")

        if backfill_start is not None or backfill_end is not None:
            config_data = {
                **config_data,
                "source": {
                    **config_data["source"],
                    "mode": "backfill",
                    "backfill_start": backfill_start,
                    "backfill_end": backfill_end,
                },
            }

        config = IngestionConfig.model_validate(config_data)

        if config.source.mode not in {"incr", "hist", "backfill"}:
            raise ValueError("source.mode must be one of 'incr', 'hist', 'backfill'")
        if config.source.mode == "incr" and not config.source.watermark_column:
            raise ValueError("source.watermark_column is required for incremental ingestion")
        if config.source.mode == "backfill":
            if config.source.backfill_start is None or config.source.backfill_end is None:
                raise ValueError(
                    "source.backfill_start and source.backfill_end are required for backfill mode"
                )
            if config.source.backfill_start < 1:
                raise ValueError("source.backfill_start must be >= 1")
            if config.source.backfill_start > config.source.backfill_end:
                raise ValueError("source.backfill_start must be <= source.backfill_end")
        if config.destination.mode != "upsert":
            raise ValueError(f"Unsupported destination mode: {config.destination.mode}")
        if not config.loading.id_cols:
            raise ValueError("loading.id_cols is required for upsert mode")
        if config.loading.batch_size is not None and config.loading.batch_size <= 0:
            raise ValueError("loading.batch_size must be greater than zero")
        if config.schema.mode != "explicit":
            raise ValueError("Currently only schema.mode='explicit' is supported")
        if not config.schema.definition:
            raise ValueError("schema.definition is required for explicit schema mode")
        if len(config.loading.id_cols) != len(set(config.loading.id_cols)):
            raise ValueError("loading.id_cols cannot contain duplicates")

        schema_path = (path.parent / config.schema.definition).resolve()
        if not schema_path.is_file():
            raise ValueError(f"Schema file does not exist: {schema_path}")

        return {
            "table_name": table_name,
            "config": config,
            "config_path": path,
            "schema_path": schema_path,
        }
    except (OSError, yaml.YAMLError, ValidationError, ValueError) as exc:
        raise ConfigurationError(f"Invalid ingestion config '{path}': {exc}") from exc
