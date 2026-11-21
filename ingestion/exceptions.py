class IngestionError(Exception):
    """Base exception for ingestion errors."""


class ConfigurationError(IngestionError):
    pass


class ExtractionError(IngestionError):
    pass


class SchemaError(IngestionError):
    pass


class PublishingError(IngestionError):
    pass


class DataQualityError(IngestionError):
    pass
