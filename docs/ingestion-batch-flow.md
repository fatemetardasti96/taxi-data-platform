# Batch ingestion flow

End-to-end batch ingestion for NYC TLC taxi trips, zone lookup reference data, and historical weather into DuckDB bronze. This diagram mirrors the ingestion design sketch and maps to `ingestion/batch`, `ingestion/historical_weather`, and dbt seeds.

## Flow diagram

```mermaid
flowchart TB
    subgraph trips [Write trips data]
        direction LR
        T1["Download from URL<br/><i>TLC Parquet per year-month</i>"]
        T2["Load Parquet into DataFrame<br/>+ preprocess<br/><i>rename columns, source_file,<br/>ingestion_ts, record_hash</i>"]
        T3["Write staging temp Parquet<br/><i>data/staging/taxi_trips/</i>"]
        T4["Load into DuckDB bronze<br/><i>read_parquet → merge by record_hash</i>"]
        T1 --> T2 --> T3 --> T4
    end

    subgraph zones [Zone lookup — lat/long for weather API]
        direction TB
        Z1["Download zone lookup CSV<br/><i>LocationID, Borough, Zone, service_zone</i>"]
        Z2["Download shapefile<br/><i>centroid per zone → latitude, longitude</i>"]
        Z3["Create bronze.taxi_zone_lookup<br/><i>dbt seed: CSV + coordinates</i>"]
        Z1 --> Z3
        Z2 --> Z3
        ZNote["Trip data has zone IDs;<br/>weather API needs lat/long"]
        Z3 -.-> ZNote
    end

    subgraph weather [Ingest weather data from API]
        direction LR
        W1["Build URL per zone<br/><i>hourly archive API<br/>start date + end date<br/>for each lat/long in lookup</i>"]
        W2["Extract weather_code<br/>from API response"]
        W3["Stage into temp table<br/><i>staging_weather_ingest</i>"]
        W4["Merge with bronze.weather<br/><i>idempotent merge<br/>bulk write, type validate</i>"]
        W1 --> W2 --> W3 --> W4
    end

  T4 --> BRONZE_TRIPS[(bronze.taxi_trips)]
  Z3 --> BRONZE_ZONES[(bronze.taxi_zone_lookup)]
  BRONZE_ZONES --> W1
  W4 --> BRONZE_WEATHER[(bronze.weather)]

  BRONZE_TRIPS -.->|"zone IDs in trips"| ZNote
```

## Simplified orchestration view

How the three flows run in the Airflow DAG (`taxi_etl`):

```mermaid
flowchart LR
    A[ingest_taxi_trips] --> B[seed_taxi_zones]
    B --> C[ingest_historical_weather]
    C --> D[transform_taxi_trips]
    D --> E[test_taxi_trips]

    A -.->|"bronze.taxi_trips"| DB[(taxi.duckdb)]
    B -.->|"bronze.taxi_zone_lookup"| DB
    C -.->|"bronze.weather"| DB
    D -.->|"silver + gold via dbt"| DB
```

## Code mapping

| Step | Implementation |
|------|----------------|
| Download TLC Parquet | `ingestion/batch/main.py` → `_download_parquet_files` |
| Transform + lineage columns | `ingestion/batch/main.py` → `_transform` |
| Staging Parquet | `ingestion/batch/main.py` → `_write_staging` |
| Merge into bronze | `ingestion/batch/duckdb_writer.py` → `DuckDBBronzeWriter.write` |
| Zone lookup + coordinates | `transformation/taxi/seeds/taxi_zone_lookup.csv` (dbt seed) |
| Weather URL + fetch | `ingestion/historical_weather/main.py` |
| Weather temp table + merge | `ingestion/historical_weather/duckdb_writer.py` |

## Idempotency

| Dataset | Dedup key | Re-run behavior |
|---------|-----------|-----------------|
| Taxi trips | `record_hash` | Delete matching hashes, then insert (no duplicates) |
| Weather | `(latitude, longitude, timestamp)` | Delete matching keys, then insert (no duplicates) |
| Zone lookup | dbt seed | Full refresh on `dbt seed` |
