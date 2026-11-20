from handlers.postgres import PostgresPublisher

PUBLISHERS = {
    "postgres": PostgresPublisher,
}


def get_publisher_handler(publish_type: str):
    try:
        return PUBLISHERS[publish_type]()
    except KeyError as exc:
        raise ValueError(f"Unsupported publisher type: {publish_type}") from exc
