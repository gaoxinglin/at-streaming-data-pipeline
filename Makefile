.PHONY: kafka-up kafka-down run-producer run-streaming run-bronze run-q1 run-q2 run-q3 run-q4 test lint

# --- Infrastructure ---

kafka-up:
	docker compose -f infrastructure/docker-compose.yml up -d

kafka-down:
	docker compose -f infrastructure/docker-compose.yml down

# --- Producer ---

run-producer:
	PYSPARK_PYTHON=$$(pwd)/.venv/bin/python .venv/bin/python -m src.ingestion.at_producer

# --- Streaming jobs ---

run-bronze:
	PYSPARK_PYTHON=$$(pwd)/.venv/bin/python .venv/bin/python -m src.streaming.bronze_ingestion

run-q1:
	PYSPARK_PYTHON=$$(pwd)/.venv/bin/python .venv/bin/python -m src.streaming.delay_alert_job

run-q2:
	PYSPARK_PYTHON=$$(pwd)/.venv/bin/python .venv/bin/python -m src.streaming.vehicle_stall_job

run-q3:
	PYSPARK_PYTHON=$$(pwd)/.venv/bin/python .venv/bin/python -m src.streaming.headway_regularity_job

run-q4:
	PYSPARK_PYTHON=$$(pwd)/.venv/bin/python .venv/bin/python -m src.streaming.alert_correlation_job

run-streaming: run-bronze run-q1 run-q2 run-q3 run-q4

# --- dbt (placeholder for M5) ---

dbt-run:
	cd dbt && dbt run

# --- Quality ---

test:
	PYSPARK_PYTHON=$$(pwd)/.venv/bin/python .venv/bin/python -m pytest tests/ -v

lint:
	.venv/bin/ruff check src/ tests/
