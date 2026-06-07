# Taxi Data Platform

NYC Yellow Taxi POC for data engineering case: ingest trip and weather data into DuckDB, transform through a medallion pipeline with dbt, and analyze revenue and weather impact.

The presentation can be found at [Google Slides](https://docs.google.com/presentation/d/1xQmKWXvFQdV-gS8ExFmOOPoQoxOQ1b7w0KzbR8A6BMM/edit?slide=id.g3ea0eaf2895_0_140#slide=id.g3ea0eaf2895_0_140).

## Repo structure

```
./
├── src/                          # Application code (Python + dbt)
│   ├── ingestion/                # Batch & streaming loaders → DuckDB bronze
│   │   ├── batch/                # TLC trip Parquet ingestion
│   │   └── historical_weather/   # Open-Meteo archive ingestion
│   ├── transformation/taxi/        # dbt project (silver + gold models)
│   └── data/                     # Local DuckDB database (gitignored)
├── docs/adr/                     # Architecture decision records
└── data viz/                     # Tableau / analysis exports
```

See [`src/README.md`](src/README.md) for setup and run instructions.
