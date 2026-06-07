# Taxi Data Platform

The presentation can be found at [google slides](https://docs.google.com/presentation/d/1xQmKWXvFQdV-gS8ExFmOOPoQoxOQ1b7w0KzbR8A6BMM/edit?slide=id.g3ea0eaf2895_0_140#slide=id.g3ea0eaf2895_0_140)

## Batch ingestion (DuckDB bronze)

1. Install dependencies:

```bash
cd src
uv sync
```

2. Run batch ingestion (downloads, transforms, and loads; idempotent merge by `record_hash`):

```bash
uv run python -m ingestion.batch.main --year 2024 --months 01
```

Re-running the same command does not duplicate rows. Use `--mode overwrite` only for a full rebuild.

3. Verify data in DuckDB:

```bash
uv run python -c "import duckdb; c=duckdb.connect('data/taxi.duckdb'); print(c.sql('SELECT COUNT(*) AS rows FROM bronze.taxi_trips').fetchall()); print(c.sql('SELECT COUNT(*) - COUNT(DISTINCT record_hash) AS duplicate_hashes FROM bronze.taxi_trips').fetchall())"
```

## Historical weather ingestion (DuckDB bronze)

Requires `bronze.taxi_zone_lookup` (load via `dbt seed` first).

```bash
uv run python -m ingestion.historical_weather.main --start-date 2026-01-01 --end-date 2026-03-31
```

Re-running with the same date range does not duplicate rows. Use `--mode overwrite` only for a full rebuild.

## dbt staging (silver layer)

```bash
export DBT_PROFILES_DIR=transformation/taxi
export DUCKDB_PATH=data/taxi.duckdb

uv run dbt run --project-dir transformation/taxi
uv run dbt test --project-dir transformation/taxi
```
