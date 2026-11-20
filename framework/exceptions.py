class FrameworkError(Exception):
    """Base exception for ingestion framework errors."""


class ConfigurationError(FrameworkError):
    pass


class ExtractionError(FrameworkError):
    pass


class SchemaError(FrameworkError):
    pass


class PublishingError(FrameworkError):
    pass


class DataQualityError(FrameworkError):
    pass
