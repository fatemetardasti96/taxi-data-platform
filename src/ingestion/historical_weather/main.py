"""Ingest historical weather data into the DuckDB bronze layer."""

import argparse
import json
import logging
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date

import duckdb

from ingestion.historical_weather.conf import HistoricalWeatherIngestionConfig
from ingestion.historical_weather.duckdb_writer import DuckDBWeatherWriter, WeatherRecord

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}


class HistoricalWeatherIngestion:
    """Fetch hourly weather archive data for taxi zones and load bronze.weather."""

    def __init__(self, config: HistoricalWeatherIngestionConfig | None = None) -> None:
        self.conf = config or HistoricalWeatherIngestionConfig()
        self.writer = DuckDBWeatherWriter(self.conf)

    def _validate_date_range(self, start_date: date, end_date: date) -> None:
        if start_date > end_date:
            raise ValueError(f"start_date ({start_date}) must be on or before end_date ({end_date})")

    def _load_zone_coordinates(self) -> list[tuple[float, float]]:
        if not self.conf.destination_duckdb_path.exists():
            raise FileNotFoundError(
                f"DuckDB database not found: {self.conf.destination_duckdb_path}. "
                "Run dbt seed to load bronze.taxi_zone_lookup first."
            )

        conn = duckdb.connect(str(self.conf.destination_duckdb_path), read_only=True)
        try:
            table_exists = conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = ? AND table_name = ?
                """,
                [self.conf.zone_lookup_schema, self.conf.zone_lookup_table_name],
            ).fetchone()[0]
            if not table_exists:
                raise FileNotFoundError(
                    f"Zone lookup table not found: {self.conf.qualified_zone_lookup_table_name}. "
                    "Run: uv run dbt seed --project-dir transformation/taxi"
                )

            rows = conn.execute(
                f"""
                SELECT DISTINCT latitude, longitude
                FROM {self.conf.qualified_zone_lookup_table_name}
                WHERE latitude IS NOT NULL
                  AND longitude IS NOT NULL
                ORDER BY latitude, longitude
                """
            ).fetchall()
        finally:
            conn.close()

        if not rows:
            raise ValueError(
                f"No coordinates found in {self.conf.qualified_zone_lookup_table_name}. "
                "Ensure the table contains latitude and longitude columns."
            )

        return [(float(latitude), float(longitude)) for latitude, longitude in rows]

    def _build_archive_url(self, latitude: float, longitude: float, start_date: date, end_date: date) -> str:
        query = urllib.parse.urlencode(
            {
                "latitude": latitude,
                "longitude": longitude,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "hourly": self.conf.hourly_variables,
                "timezone": self.conf.timezone,
            }
        )
        return f"{self.conf.archive_api_base_url}?{query}"

    def _fetch_archive_payload(
        self,
        latitude: float,
        longitude: float,
        start_date: date,
        end_date: date,
    ) -> dict:
        url = self._build_archive_url(latitude, longitude, start_date, end_date)
        last_error: Exception | None = None

        for attempt in range(1, self.conf.max_retries + 1):
            request = urllib.request.Request(url, headers={"Accept": "application/json"})
            try:
                with urllib.request.urlopen(request, timeout=self.conf.request_timeout_seconds) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code not in RETRYABLE_HTTP_CODES or attempt == self.conf.max_retries:
                    raise ConnectionError(
                        f"Failed to fetch weather data for ({latitude}, {longitude}): HTTP {exc.code}"
                    ) from exc
                sleep_seconds = self.conf.retry_backoff_seconds * attempt
                logger.warning(
                    "Retryable HTTP %s for (%s, %s), attempt %s/%s; sleeping %.1fs",
                    exc.code,
                    latitude,
                    longitude,
                    attempt,
                    self.conf.max_retries,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
                continue
            except urllib.error.URLError as exc:
                last_error = exc
                if attempt == self.conf.max_retries:
                    raise ConnectionError(
                        f"Failed to fetch weather data for ({latitude}, {longitude}): {exc.reason}"
                    ) from exc
                sleep_seconds = self.conf.retry_backoff_seconds * attempt
                logger.warning(
                    "Network error for (%s, %s), attempt %s/%s; sleeping %.1fs",
                    latitude,
                    longitude,
                    attempt,
                    self.conf.max_retries,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
                continue
            else:
                if payload.get("error"):
                    raise ValueError(
                        f"Open-Meteo API error for ({latitude}, {longitude}): "
                        f"{payload.get('reason', 'unknown error')}"
                    )
                return payload

        raise ConnectionError(
            f"Failed to fetch weather data for ({latitude}, {longitude}) after "
            f"{self.conf.max_retries} attempts"
        ) from last_error

    def _parse_weather_response(
        self,
        payload: dict,
        latitude: float,
        longitude: float,
    ) -> list[WeatherRecord]:
        hourly = payload.get("hourly")
        if not hourly:
            raise ValueError(f"Missing hourly data in API response for ({latitude}, {longitude})")

        timestamps = hourly.get("time")
        weather_codes = hourly.get("weather_code")
        if not timestamps or not weather_codes:
            raise ValueError(f"Missing hourly weather_code data for ({latitude}, {longitude})")
        if len(timestamps) != len(weather_codes):
            raise ValueError(
                f"Mismatched hourly arrays for ({latitude}, {longitude}): "
                f"{len(timestamps)} timestamps vs {len(weather_codes)} weather codes"
            )

        generationtime_ms = float(payload["generationtime_ms"])
        return [
            WeatherRecord(
                latitude=latitude,
                longitude=longitude,
                timestamp=timestamp,
                weather_code=int(weather_code),
                generationtime_ms=generationtime_ms,
            )
            for timestamp, weather_code in zip(timestamps, weather_codes)
        ]

    def _fetch_weather_for_zones(
        self,
        zones: list[tuple[float, float]],
        start_date: date,
        end_date: date,
    ) -> list[WeatherRecord]:
        records: list[WeatherRecord] = []
        total_zones = len(zones)

        for index, (latitude, longitude) in enumerate(zones, start=1):
            logger.info(
                "Fetching weather for zone %s/%s at (%.6f, %.6f)",
                index,
                total_zones,
                latitude,
                longitude,
            )
            payload = self._fetch_archive_payload(latitude, longitude, start_date, end_date)
            zone_records = self._parse_weather_response(payload, latitude, longitude)
            records.extend(zone_records)
            logger.info("Fetched %s hourly rows for (%.6f, %.6f)", len(zone_records), latitude, longitude)

            if index < total_zones and self.conf.request_delay_seconds > 0:
                time.sleep(self.conf.request_delay_seconds)

        return records

    def launch(self, start_date: date, end_date: date, mode: str = "merge") -> int:
        self._validate_date_range(start_date, end_date)
        zones = self._load_zone_coordinates()
        logger.info(
            "Starting weather ingestion for %s zone(s) from %s to %s",
            len(zones),
            start_date,
            end_date,
        )

        records = self._fetch_weather_for_zones(zones, start_date, end_date)
        logger.info("Fetched %s total hourly weather rows", len(records))
        return self.writer.write(records, mode=mode)


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Expected YYYY-MM-DD, got {value!r}") from exc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    default_conf = HistoricalWeatherIngestionConfig()
    parser = argparse.ArgumentParser(
        description="Ingest historical hourly weather data into DuckDB bronze.weather"
    )
    parser.add_argument(
        "--mode",
        choices=["merge", "overwrite"],
        default="merge",
        help=(
            "merge: upsert by latitude, longitude, and timestamp (idempotent, default); "
            "overwrite: drop and recreate bronze.weather"
        ),
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        default=default_conf.default_start_date,
        help=f"Inclusive start date (default: {default_conf.default_start_date.isoformat()})",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=default_conf.default_end_date,
        help=f"Inclusive end date (default: {default_conf.default_end_date.isoformat()})",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    ingestion = HistoricalWeatherIngestion()
    row_count = ingestion.launch(
        start_date=args.start_date,
        end_date=args.end_date,
        mode=args.mode,
    )
    logger.info(
        "Weather ingestion complete. DuckDB: %s, table: %s, rows: %s",
        ingestion.conf.destination_duckdb_path,
        ingestion.conf.qualified_table_name,
        row_count,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
