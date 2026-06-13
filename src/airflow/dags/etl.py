"""
Monthly DAG that ingests taxi trips and historical weather data from the
previous month into the DuckDB bronze layer, then runs dbt models and tests.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow"
DBT_ENV = (
    f"export DBT_PROFILES_DIR={PROJECT_DIR}/transformation/taxi "
    f"DUCKDB_PATH={PROJECT_DIR}/data/taxi.duckdb"
)

INGEST_PERIOD = """
if [[ -n "${TAXI_INGEST_YEAR:-}" && -n "${TAXI_INGEST_MONTH:-}" ]]; then
  YEAR="$TAXI_INGEST_YEAR"
  MONTH="$TAXI_INGEST_MONTH"
  PREV_FIRST="${YEAR}-${MONTH}-01"
  PREV_LAST="$(date -d "${PREV_FIRST} +1 month -1 day" +%Y-%m-%d)"
else
  PREV_FIRST=$(date -d "$(date +%Y-%m-01) -1 month" +%Y-%m-01)
  PREV_LAST=$(date -d "$(date +%Y-%m-01) -1 day" +%Y-%m-%d)
  YEAR=$(date -d "$PREV_FIRST" +%Y)
  MONTH=$(date -d "$PREV_FIRST" +%m)
fi
if [[ -n "${WEATHER_START_DATE:-}" && -n "${WEATHER_END_DATE:-}" ]]; then
  PREV_FIRST="$WEATHER_START_DATE"
  PREV_LAST="$WEATHER_END_DATE"
fi
"""

with DAG(
    dag_id="taxi_etl",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule="@monthly",
    catchup=False,
    dagrun_timeout=timedelta(hours=3),
    max_active_runs=1,
    start_date=datetime(2026, 1, 1),
    tags=["taxi", "etl"],
) as dag:
    ingest_taxi_trips = BashOperator(
        task_id="ingest_taxi_trips",
        bash_command=f"""
set -euo pipefail
cd {PROJECT_DIR}
{INGEST_PERIOD}
echo "[ingest_taxi_trips] Starting ingestion for year=$YEAR month=$MONTH"
echo "[ingest_taxi_trips] DuckDB path: {PROJECT_DIR}/data/taxi.duckdb"
python -m ingestion.batch.main --year "$YEAR" --months "$MONTH"
echo "[ingest_taxi_trips] Completed successfully"
""",
    )

    seed_taxi_zones = BashOperator(
        task_id="seed_taxi_zones",
        bash_command=f"""
set -euo pipefail
cd {PROJECT_DIR}
{DBT_ENV}
echo "[seed_taxi_zones] Running dbt seed"
dbt seed --project-dir transformation/taxi
echo "[seed_taxi_zones] Completed successfully"
""",
    )

    ingest_historical_weather = BashOperator(
        task_id="ingest_historical_weather",
        bash_command=f"""
set -euo pipefail
cd {PROJECT_DIR}
{INGEST_PERIOD}
echo "[ingest_historical_weather] Starting ingestion from $PREV_FIRST to $PREV_LAST"
echo "[ingest_historical_weather] DuckDB path: {PROJECT_DIR}/data/taxi.duckdb"
python -m ingestion.historical_weather.main --start-date "$PREV_FIRST" --end-date "$PREV_LAST"
echo "[ingest_historical_weather] Completed successfully"
""",
    )

    transform_taxi_trips = BashOperator(
        task_id="transform_taxi_trips",
        bash_command=f"""
set -euo pipefail
cd {PROJECT_DIR}
{DBT_ENV}
echo "[transform_taxi_trips] Running dbt models (DUCKDB_PATH=$DUCKDB_PATH)"
dbt run --project-dir transformation/taxi
echo "[transform_taxi_trips] Completed successfully"
""",
    )

    test_taxi_trips = BashOperator(
        task_id="test_taxi_trips",
        bash_command=f"""
set -euo pipefail
cd {PROJECT_DIR}
{DBT_ENV}
echo "[test_taxi_trips] Running dbt tests"
dbt test --project-dir transformation/taxi
echo "[test_taxi_trips] Completed successfully"
""",
    )

    (
        ingest_taxi_trips
        >> seed_taxi_zones
        >> ingest_historical_weather
        >> transform_taxi_trips
        >> test_taxi_trips
    )
