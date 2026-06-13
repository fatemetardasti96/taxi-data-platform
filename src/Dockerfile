FROM apache/airflow:3.2.2

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libffi-dev \
    libssl-dev \
    default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/airflow/.cache/uv && chown -R airflow:root /opt/airflow/.cache

USER airflow

WORKDIR /opt/airflow

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV DBT_PROFILES_DIR=/opt/airflow/transformation/taxi
ENV DUCKDB_PATH=/opt/airflow/data/taxi.duckdb
ENV UV_CACHE_DIR=/opt/airflow/.cache/uv
ENV PYTHONPATH=/opt/airflow
ENV PATH="/opt/airflow/.venv/bin:${PATH}"

RUN pip install --no-cache-dir uv==0.8.17

COPY pyproject.toml uv.lock README.md ./
COPY ingestion ./ingestion

RUN uv sync --no-dev --frozen

COPY transformation ./transformation
COPY airflow ./airflow
