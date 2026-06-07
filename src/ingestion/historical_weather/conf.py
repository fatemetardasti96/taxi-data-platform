from dataclasses import dataclass
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class HistoricalWeatherIngestionConfig:
    """Configuration for historical weather ingestion into the DuckDB bronze layer."""

    archive_api_base_url: str = "https://archive-api.open-meteo.com/v1/archive"
    timezone: str = "America/New_York"
    hourly_variables: str = "weather_code"
    default_start_date: date = date(2026, 1, 1)
    default_end_date: date = date(2026, 3, 31)
    destination_duckdb_path: Path = PROJECT_ROOT / "data" / "taxi.duckdb"
    destination_schema: str = "bronze"
    destination_table_name: str = "weather"
    zone_lookup_schema: str = "bronze"
    zone_lookup_table_name: str = "taxi_zone_lookup"
    request_timeout_seconds: int = 30
    request_delay_seconds: float = 0.2
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0

    @property
    def qualified_table_name(self) -> str:
        return f"{self.destination_schema}.{self.destination_table_name}"

    @property
    def qualified_zone_lookup_table_name(self) -> str:
        return f"{self.zone_lookup_schema}.{self.zone_lookup_table_name}"
