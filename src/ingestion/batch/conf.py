from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class HistoricalDataIngestionConfig:
    """Configuration for batch ingestion into the DuckDB bronze layer."""

    column_mapping: dict[str, str] = field(
        default_factory=lambda: {
            "VendorID": "vendor_id",
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
            "passenger_count": "passenger_count",
            "trip_distance": "trip_distance",
            "RatecodeID": "rate_code_id",
            "store_and_fwd_flag": "store_and_fwd_flag",
            "PULocationID": "pickup_location_id",
            "DOLocationID": "dropoff_location_id",
            "payment_type": "payment_type",
            "fare_amount": "fare_amount",
            "extra": "extra",
            "mta_tax": "mta_tax",
            "tip_amount": "tip_amount",
            "tolls_amount": "tolls_amount",
            "improvement_surcharge": "improvement_surcharge",
            "total_amount": "total_amount",
            "congestion_surcharge": "congestion_surcharge",
            "Airport_fee": "airport_fee",
            "airport_fee": "airport_fee",
            "trip_type": "trip_type",
            "cbd_congestion_fee": "cbd_congestion_fee",
        }
    )

    record_hash_key_columns: tuple[str, ...] = (
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "store_and_fwd_flag",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "trip_type",
        "cbd_congestion_fee",
    )

    trip_data_base_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    taxi_type: str = "yellow"
    download_dir: Path = PROJECT_ROOT / "data" / "downloads" / "yellow"
    staging_dir: Path = PROJECT_ROOT / "data" / "staging" / "taxi_trips"
    destination_duckdb_path: Path = PROJECT_ROOT / "data" / "taxi.duckdb"
    destination_schema: str = "bronze"
    destination_table_name: str = "taxi_trips"

    @property
    def qualified_table_name(self) -> str:
        return f"{self.destination_schema}.{self.destination_table_name}"
