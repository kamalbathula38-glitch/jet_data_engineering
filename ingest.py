import argparse
import json

from dotenv import load_dotenv

from ingestion.base_ingestion import BaseIngestion
from ingestion.config_parser import load_config


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Run a configured data ingestion")
    parser.add_argument(
        "--config",
        default="config/ingestions/xkcd.yaml",
        help="Path to an ingestion YAML file",
    )
    parser.add_argument(
        "--backfill-start",
        type=int,
        default=None,
        help="Ad-hoc backfill: first comic number to ingest (requires --backfill-end)",
    )
    parser.add_argument(
        "--backfill-end",
        type=int,
        default=None,
        help="Ad-hoc backfill: last comic number to ingest (requires --backfill-start)",
    )
    args = parser.parse_args()

    if (args.backfill_start is None) != (args.backfill_end is None):
        parser.error("--backfill-start and --backfill-end must be provided together")

    parsed_config = load_config(
        args.config,
        backfill_start=args.backfill_start,
        backfill_end=args.backfill_end,
    )
    result = BaseIngestion(parsed_config).run()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
