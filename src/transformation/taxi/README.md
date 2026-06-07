# dbt transformation

Run from the `src` directory:

```bash
cd src
export DBT_PROFILES_DIR=transformation/taxi
export DUCKDB_PATH=data/taxi.duckdb

uv run dbt run --project-dir transformation/taxi
uv run dbt test --project-dir transformation/taxi
```

## Layers

| Model | Schema | Purpose |
|-------|--------|---------|
| `stg_taxi_trips` | silver | 1:1 bronze mapping, type casting only |
| `prep_taxi_trips` | silver | Cleansing, normalization, data quality tests |
