import argparse
import logging
import sys
import urllib.error
import urllib.request
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, current_timestamp, input_file_name, lit, sha2

from ingestion.batch.conf import HistoricalDataIngestionConfig
from ingestion.batch.duckdb_writer import DuckDBBronzeWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


class HistoricalDataIngestion:
    """Batch-ingest historical TLC Parquet files into the DuckDB bronze layer."""

    def __init__(self, spark: SparkSession, config: HistoricalDataIngestionConfig | None = None) -> None:
        self.spark = spark
        self.conf = config or HistoricalDataIngestionConfig()
        self.writer = DuckDBBronzeWriter(self.conf)

    def _trip_data_filename(self, year: int, month: str) -> str:
        return f"{self.conf.taxi_type}_tripdata_{year}-{month}.parquet"

    def _trip_data_url(self, year: int, month: str) -> str:
        return f"{self.conf.trip_data_base_url}/{self._trip_data_filename(year, month)}"

    def _download_parquet_file(self, url: str, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading %s to %s", url, destination)
        try:
            urllib.request.urlretrieve(url, destination)
        except urllib.error.HTTPError as exc:
            raise FileNotFoundError(f"Failed to download {url}: HTTP {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise ConnectionError(f"Failed to download {url}: {exc.reason}") from exc
        return destination

    def _download_parquet_files(self, year: int, months: list[str]) -> list[Path]:
        if not months:
            raise ValueError("At least one month must be provided")

        downloaded_files: list[Path] = []
        for month in months:
            destination = self.conf.download_dir / self._trip_data_filename(year, month)
            if destination.exists():
                logger.info("Using cached file: %s", destination)
            else:
                try:
                    self._download_parquet_file(self._trip_data_url(year, month), destination)
                except Exception as e:
                    logger.error("Error downloading %s: %s", destination, e)
                    continue
            downloaded_files.append(destination)

        return sorted(downloaded_files)

    def _rename_columns(self, df: DataFrame) -> DataFrame:
        for old_name, new_name in self.conf.column_mapping.items():
            if old_name in df.columns and old_name != new_name:
                df = df.withColumnRenamed(old_name, new_name)
        return df

    def _add_lineage_columns(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("ingestion_ts", current_timestamp())
            .withColumn("source_type", lit("batch_backfill"))
            .withColumn("source_file", input_file_name())
        )

    def _add_record_hash(self, df: DataFrame) -> DataFrame:
        hash_columns = [
            column_name
            for column_name in self.conf.record_hash_key_columns
            if column_name in df.columns
        ]
        if not hash_columns:
            raise ValueError("No record_hash key columns found in dataframe")

        hash_input = concat_ws(
            "|",
            *[
                coalesce(col(column_name).cast("string"), lit(""))
                for column_name in hash_columns
            ],
        )
        return df.withColumn("record_hash", sha2(hash_input, 256))

    def _transform(self, df: DataFrame) -> DataFrame:
        df = self._rename_columns(df)
        df = self._add_lineage_columns(df)
        df = self._add_record_hash(df)
        return df.dropDuplicates(["record_hash"])

    def _write_staging(self, df: DataFrame) -> Path:
        self.conf.staging_dir.mkdir(parents=True, exist_ok=True)
        staging_path = self.conf.staging_dir
        logger.info("Writing transformed batch data to staging path: %s", staging_path)
        df.write.mode("overwrite").parquet(str(staging_path))
        return staging_path

    def launch(self, mode: str = "merge", year: int = 2024, months: list[str] | None = None) -> int:
        months = months or ["01"]
        parquet_files = self._download_parquet_files(year, months)
        logger.info("Processing %s Parquet file(s) for %s", len(parquet_files), f"{year}-{','.join(months)}")

        source_paths = [str(path) for path in parquet_files]
        raw_df = self.spark.read.parquet(*source_paths)
        transformed_df = self._transform(raw_df)

        row_count = transformed_df.count()
        logger.info("Transformed %s rows from batch source files", row_count)

        staging_path = self._write_staging(transformed_df)
        loaded_rows = self.writer.write(staging_path, mode=mode)
        return loaded_rows


def build_spark(app_name: str = "HistoricalDataIngestion") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch ingest TLC data into DuckDB bronze layer")
    parser.add_argument(
        "--mode",
        choices=["merge", "overwrite"],
        default="merge",
        help=(
            "merge: upsert by record_hash (idempotent, default); "
            "overwrite: drop and recreate bronze table"
        ),
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2026,
        help="Trip data year to download and ingest (default: 2024)",
    )
    parser.add_argument(
        "--months",
        nargs="+",
        default=["01"],
        help="One or more trip data months to download and ingest (e.g. 01 02 03)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    spark = build_spark()
    try:
        ingestion = HistoricalDataIngestion(spark=spark)
        row_count = ingestion.launch(mode=args.mode, year=args.year, months=args.months)
        logger.info(
            "Batch ingestion complete. DuckDB: %s, table: %s, rows: %s",
            ingestion.conf.destination_duckdb_path,
            ingestion.conf.qualified_table_name,
            row_count,
        )
        return 0
    finally:
        try:
            spark.stop()
        except Exception:
            logger.warning("Spark session shutdown raised an error; ingestion already completed")


if __name__ == "__main__":
    sys.exit(main())
