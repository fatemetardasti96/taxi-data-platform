import logging
from pathlib import Path

import duckdb

from ingestion.batch.conf import HistoricalDataIngestionConfig

logger = logging.getLogger(__name__)

STAGING_TABLE = "staging_batch_ingest"


class DuckDBBronzeWriter:
    """Persist batch staging Parquet files into the DuckDB bronze layer."""

    def __init__(self, config: HistoricalDataIngestionConfig) -> None:
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

    def _load_staging(self, conn: duckdb.DuckDBPyConnection, parquet_glob: str) -> int:
        conn.execute(
            f"CREATE OR REPLACE TEMP TABLE {STAGING_TABLE} AS SELECT * FROM read_parquet(?)",
            [parquet_glob],
        )
        staging_count = conn.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}").fetchone()[0]
        logger.info("Loaded %s rows into temp staging table", staging_count)
        return staging_count

    def _merge_staging(self, conn: duckdb.DuckDBPyConnection) -> int:
        staging_count = conn.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}").fetchone()[0]
        conn.execute(
            f"""
            DELETE FROM {self.config.qualified_table_name} AS target
            WHERE target.record_hash IN (
                SELECT record_hash FROM {STAGING_TABLE}
            )
            """
        )
        conn.execute(
            f"""
            INSERT INTO {self.config.qualified_table_name}
            SELECT * FROM {STAGING_TABLE}
            """
        )
        logger.info("Merged %s staged rows by record_hash", staging_count)
        return staging_count

    def write(self, staging_dir: Path, mode: str = "merge") -> int:
        parquet_glob = str(staging_dir / "*.parquet")
        if not list(staging_dir.glob("*.parquet")):
            raise FileNotFoundError(f"No Parquet files found in staging directory: {staging_dir}")

        conn = duckdb.connect(str(self.config.destination_duckdb_path))
        try:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.config.destination_schema}")
            staging_rows = self._load_staging(conn, parquet_glob)

            if mode == "overwrite":
                conn.execute(f"DROP TABLE IF EXISTS {self.config.qualified_table_name}")
                conn.execute(
                    f"""
                    CREATE TABLE {self.config.qualified_table_name} AS
                    SELECT * FROM {STAGING_TABLE}
                    """
                )
            elif mode == "merge":
                if not self._table_exists(conn):
                    conn.execute(
                        f"""
                        CREATE TABLE {self.config.qualified_table_name} AS
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
            duplicate_hashes = conn.execute(
                f"""
                SELECT COUNT(*) - COUNT(DISTINCT record_hash)
                FROM {self.config.qualified_table_name}
                """
            ).fetchone()[0]
            if duplicate_hashes:
                raise RuntimeError(
                    f"Duplicate record_hash values detected in {self.config.qualified_table_name}: "
                    f"{duplicate_hashes}"
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
