from datetime import date
import time

import requests

from ingestion.exceptions import ExtractionError
from utils.retry import retry_with_backoff


class XKCDIngestion:
    BASE_URL = "https://xkcd.com"

    def __init__(self, timeout: int = 20, request_delay: float = 0.0):
        self.timeout = timeout
        self.request_delay = request_delay
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "jet-data-engineering-assignment/1.0"})

    @retry_with_backoff(max_retries=3, base_delay=1.0, exceptions=(requests.RequestException,))
    def _request(self, url: str) -> requests.Response:
        response = self.session.get(url, timeout=self.timeout)
        # A 404 means the comic doesn't exist (e.g. #404 itself) - not a
        # transient failure, so it's left for the caller and never retried.
        if response.status_code != 404:
            response.raise_for_status()
        return response

    def _get_json(self, url: str) -> dict | None:
        try:
            response = self._request(url)
            if response.status_code == 404:
                return None
            return response.json()
        except requests.RequestException as exc:
            raise ExtractionError(f"XKCD API request failed for {url}: {exc}") from exc
        except ValueError as exc:
            raise ExtractionError(f"XKCD API returned invalid JSON for {url}") from exc

    @staticmethod
    def _normalise(payload: dict) -> dict:
        try:
            published = date(
                int(payload["year"]), int(payload["month"]), int(payload["day"])
            )
            return {
                "comic_num": int(payload["num"]),
                "title": payload["safe_title"],
                "image_url": payload["img"],
                "alt_text": payload.get("alt"),
                "transcript": payload.get("transcript") or None,
                "published_date": published,
            }
        except (KeyError, TypeError, ValueError) as exc:
            raise ExtractionError(f"Unexpected XKCD response structure: {exc}") from exc

    def get_latest(self) -> dict:
        """
        Return the most recently published comic's metadata, without
        ingesting it. Used by orchestration to detect when today's comic
        has gone live, since XKCD doesn't publish at a fixed time of day.
        """
        payload = self._get_json(f"{self.BASE_URL}/info.0.json")
        if not payload:
            raise ExtractionError("Could not retrieve the latest XKCD comic")
        return self._normalise(payload)

    def extract(self, mode: str, config: dict, watermark=None):
        latest_payload = self._get_json(f"{self.BASE_URL}/info.0.json")
        if not latest_payload:
            raise ExtractionError("Could not retrieve the latest XKCD comic")
        latest_num = int(latest_payload["num"])

        if mode == "hist":
            start_num, end_num = 1, latest_num
        elif mode == "incr":
            # First run: ingest the latest comic. Later runs: ingest IDs after watermark.
            start_num = 1 if watermark is None else int(watermark) + 1
            end_num = latest_num
        elif mode == "backfill":
            source_config = config["source"]
            start_num = int(source_config["backfill_start"])
            end_num = min(int(source_config["backfill_end"]), latest_num)
        else:
            raise ExtractionError(f"Unsupported XKCD extraction mode: {mode}")

        for comic_num in range(start_num, end_num + 1):
            payload = latest_payload if comic_num == latest_num else self._get_json(
                f"{self.BASE_URL}/{comic_num}/info.0.json"
            )
            if payload is not None:
                yield self._normalise(payload)
            if self.request_delay:
                time.sleep(self.request_delay)
