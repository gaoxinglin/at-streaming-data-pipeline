-include .env
export

.PHONY: kafka-up kafka-down run-producer run-streaming run-bronze run-q1 run-q2 run-q3 run-q4 query-bronze sync-duckdb dbt-deps dbt-seed dbt-run dbt-test dbt-compile dbt-staging dbt-docs test lint

# --- Infrastructure ---

kafka-up:
	docker compose -f infrastructure/docker-compose.yml up -d

kafka-down:
	docker compose -f infrastructure/docker-compose.yml down

# --- Producer ---

run-producer:
	PYSPARK_PYTHON=$$(pwd)/.venv/bin/python .venv/bin/python -m src.ingestion.at_producer

# --- Streaming jobs ---
# Local demo defaults. Keep them small enough for WSL + Docker, but leave an
# escape hatch in .env or the shell when you need more headroom.
SPARK_DRIVER_MEMORY ?= 1280m
Q4_SPARK_DRIVER_MEMORY ?= 1536m
SPARK_SQL_SHUFFLE_PARTITIONS ?= 4
SPARK_ENV = PYSPARK_PYTHON=$$(pwd)/.venv/bin/python SPARK_DRIVER_MEMORY=$(SPARK_DRIVER_MEMORY) SPARK_SQL_SHUFFLE_PARTITIONS=$(SPARK_SQL_SHUFFLE_PARTITIONS)
Q4_SPARK_ENV = PYSPARK_PYTHON=$$(pwd)/.venv/bin/python SPARK_DRIVER_MEMORY=$(Q4_SPARK_DRIVER_MEMORY) SPARK_SQL_SHUFFLE_PARTITIONS=$(SPARK_SQL_SHUFFLE_PARTITIONS)

run-bronze:
	$(SPARK_ENV) .venv/bin/python -m src.streaming.bronze_ingestion

run-q1:
	$(SPARK_ENV) .venv/bin/python -m src.streaming.delay_alert_job

run-q2:
	$(SPARK_ENV) .venv/bin/python -m src.streaming.vehicle_stall_job

run-q3:
	$(SPARK_ENV) .venv/bin/python -m src.streaming.headway_regularity_job

run-q4:
	$(Q4_SPARK_ENV) .venv/bin/python -m src.streaming.alert_correlation_job

run-streaming: run-bronze run-q1 run-q2 run-q3 run-q4

# --- DuckDB ---

query-bronze:
	.venv/bin/python scripts/query_bronze.py

sync-duckdb:
	.venv/bin/python scripts/sync_to_duckdb.py

# --- dbt ---

dbt-deps:
	cd transform && dbt deps

dbt-seed:
	cd transform && dbt seed

dbt-run:
	cd transform && dbt run

dbt-test:
	cd transform && dbt test

dbt-compile:
	cd transform && dbt compile

dbt-staging:
	cd transform && dbt seed && dbt run --select staging

dbt-docs:
	cd transform && dbt docs generate && dbt docs serve

# --- Quality ---

test:
	PYSPARK_PYTHON=$$(pwd)/.venv/bin/python .venv/bin/python -m pytest tests/ -v

lint:
	.venv/bin/ruff check src/ tests/
