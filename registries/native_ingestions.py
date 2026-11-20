from handlers.xkcd import XKCDIngestion

INGESTIONS = {
    "xkcd": XKCDIngestion,
}


def get_ingestion_handler(ingest_type: str):
    try:
        return INGESTIONS[ingest_type]()
    except KeyError as exc:
        raise ValueError(f"Unsupported ingestion type: {ingest_type}") from exc
