import logging
from dataclasses import dataclass

import duckdb

from ingestion.historical_weather.conf import HistoricalWeatherIngestionConfig

logger = logging.getLogger(__name__)

STAGING_TABLE = "staging_weather_ingest"


@dataclass(frozen=True)
class WeatherRecord:
    latitude: float
    longitude: float
    timestamp: str
    weather_code: int
    generationtime_ms: float


class DuckDBWeatherWriter:
    """Persist weather records into the DuckDB bronze layer."""

    def __init__(self, config: HistoricalWeatherIngestionConfig) -> None:
        self.config = config
        self.config.destination_duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    def _table_exists(self, conn: duckdb.DuckDBPyConnection) -> bool:
        result = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [self.config.destination_schema, self.config.destination_table_name],
        ).fetchone()
        return bool(result and result[0])

    def _create_weather_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(
            f"""
            CREATE TABLE {self.config.qualified_table_name} (
                latitude DOUBLE,
                longitude DOUBLE,
                timestamp TIMESTAMP,
                weather_code INTEGER,
                generationtime_ms DOUBLE
            )
            """
        )

    def _load_staging(self, conn: duckdb.DuckDBPyConnection, records: list[WeatherRecord]) -> int:
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE {STAGING_TABLE} (
                latitude DOUBLE,
                longitude DOUBLE,
                timestamp TIMESTAMP,
                weather_code INTEGER,
                generationtime_ms DOUBLE
            )
            """
        )
        conn.executemany(
            f"""
            INSERT INTO {STAGING_TABLE} (
                latitude, longitude, timestamp, weather_code, generationtime_ms
            )
            VALUES (?, ?, CAST(? AS TIMESTAMP), ?, ?)
            """,
            [
                (
                    record.latitude,
                    record.longitude,
                    record.timestamp,
                    record.weather_code,
                    record.generationtime_ms,
                )
                for record in records
            ],
        )
        staging_count = conn.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}").fetchone()[0]
        logger.info("Loaded %s rows into temp staging table", staging_count)
        return staging_count

    def _merge_staging(self, conn: duckdb.DuckDBPyConnection) -> int:
        staging_count = conn.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}").fetchone()[0]
        conn.execute(
            f"""
            DELETE FROM {self.config.qualified_table_name} AS target
            USING {STAGING_TABLE} AS staging
            WHERE target.latitude = staging.latitude
              AND target.longitude = staging.longitude
              AND target.timestamp = staging.timestamp
            """
        )
        conn.execute(
            f"""
            INSERT INTO {self.config.qualified_table_name}
            SELECT * FROM {STAGING_TABLE}
            """
        )
        logger.info("Merged %s staged rows by latitude, longitude, and timestamp", staging_count)
        return staging_count

    def write(self, records: list[WeatherRecord], mode: str = "merge") -> int:
        if not records:
            raise ValueError("No weather records to write")

        conn = duckdb.connect(str(self.config.destination_duckdb_path))
        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.config.destination_schema}")
            staging_rows = self._load_staging(conn, records)

            if mode == "overwrite":
                conn.execute(f"DROP TABLE IF EXISTS {self.config.qualified_table_name}")
                self._create_weather_table(conn)
                conn.execute(
                    f"""
                    INSERT INTO {self.config.qualified_table_name}
                    SELECT * FROM {STAGING_TABLE}
                    """
                )
            elif mode == "merge":
                if not self._table_exists(conn):
                    self._create_weather_table(conn)
                    conn.execute(
                        f"""
                        INSERT INTO {self.config.qualified_table_name}
                        SELECT * FROM {STAGING_TABLE}
                        """
                    )
                    logger.info("Created bronze table with %s rows", staging_rows)
                else:
                    self._merge_staging(conn)
            else:
                raise ValueError(f"Unsupported write mode: {mode}. Use 'merge' or 'overwrite'.")

            row_count = conn.execute(
                f"SELECT COUNT(*) FROM {self.config.qualified_table_name}"
            ).fetchone()[0]
            duplicate_rows = conn.execute(
                f"""
                SELECT COUNT(*) - COUNT(
                    DISTINCT (latitude, longitude, timestamp)
                )
                FROM {self.config.qualified_table_name}
                """
            ).fetchone()[0]
            if duplicate_rows:
                raise RuntimeError(
                    f"Duplicate weather rows detected in {self.config.qualified_table_name}: "
                    f"{duplicate_rows}"
                )

            logger.info(
                "Bronze table %s now contains %s rows (%s mode)",
                self.config.qualified_table_name,
                row_count,
                mode,
            )
            return row_count
        finally:
            conn.close()
