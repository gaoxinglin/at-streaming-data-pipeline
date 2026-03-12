# Auckland Transport Real-Time Monitoring Platform
## DE Zoomcamp Capstone — Design Document

---

## 1. Objective

Build an end-to-end **streaming-first** data platform that ingests Auckland Transport (AT) public transport data via **Kafka and Spark Structured Streaming**, stores processed data in a cloud data lake and warehouse, transforms metrics with **dbt**, and visualises key transit insights on an interactive dashboard.

This project is positioned as a **Streaming Processing showcase**, complementing the NZ Electricity project (Batch). The focus is on streaming depth: exactly-once semantics, watermarking, late data handling, and schema evolution — not breadth of technology stack.

The platform answers questions such as:
- Which bus and train routes suffer the most delays?
- What are the peak congestion windows by route and time of day?
- How do real-time vehicle positions compare to scheduled arrival times?
- Which stops have the worst on-time performance over the past 30 days?

---

## 2. Cloud Platform Choice

> **Note on platform selection:** This project uses **Google Cloud Platform (GCP)** — specifically GCS and BigQuery — because GCP offers a generous free tier suitable for personal portfolio projects. In a production or enterprise setting, the same architecture applies to other platforms:
> - **Snowflake** — increasingly adopted in NZ enterprises (banking, insurance, retail) as the warehouse layer
> - **AWS** — S3 + Kinesis / MSK + Redshift as equivalent streaming stack
> - **Azure** — ADLS Gen2 + Event Hubs + Synapse, widely used in NZ government and financial sectors
> - **Confluent Cloud** — managed Kafka as a replacement for self-hosted Kafka in Docker
>
> The pipeline design is **platform-agnostic** — Terraform, Airflow, Spark, and dbt adapters can be swapped with minimal code changes.

---

## 3. Data Sources

### 3.1 AT Developer Portal (https://dev-portal.at.govt.nz)
Free registration required. API key issued immediately.

| API                           | Type      | Update Frequency   | Format          |
| ----------------------------- | --------- | ------------------ | --------------- |
| **GTFS Static**               | Batch     | Weekly (Wednesday) | ZIP → CSV/TXT   |
| **GTFS-RT Trip Updates**      | Streaming | Every 30 seconds   | JSON / Protobuf |
| **GTFS-RT Vehicle Positions** | Streaming | Every 30 seconds   | JSON / Protobuf |
| **GTFS-RT Service Alerts**    | Streaming | On-demand          | JSON / Protobuf |

### 3.2 Scope
- **Mode**: Bus + Train (Auckland Metro rail network)
- **Geographic scope**: Auckland region only

---

## 4. Architecture Overview

The key design decision: **remove the Spark Batch layer**. Daily aggregation (OTP metrics, delay summaries) is handled by **dbt inside BigQuery**, which is more cost-effective and simpler for this scale. Spark's role is focused purely on **streaming ingestion and enrichment**.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                        │
│  AT GTFS Static (weekly ZIP)        AT GTFS-RT APIs (every 30s)             │
└──────────────┬──────────────────────────────────┬───────────────────────────┘
               │ batch (Airflow)                   │ streaming
               ▼                                   ▼
┌──────────────────────┐              ┌────────────────────────────────────┐
│ Airflow DAG          │              │ Kafka (Docker) + Schema Registry   │
│ (weekly schedule)    │              │ Topics (Avro/Protobuf):            │
│ - Download ZIP       │              │  at.trip_updates                   │
│ - Extract CSVs       │              │  at.vehicle_positions              │
│ - Upload to GCS/raw  │              │  at.service_alerts                 │
│ - Load dim tables    │              └───────────┬────────────────────────┘
│   into BigQuery      │                          │
└──────────┬───────────┘                          ▼
           │                          ┌────────────────────────────────────┐
           │                          │ Spark Structured Streaming         │
           │                          │ - Consume from Kafka topics        │
           │                          │ - Deserialise Avro/Protobuf        │
           │                          │ - Enrich with dim tables (join)    │
           │                          │ - Watermark: 10 min for late data  │
           │                          │ - Write Parquet micro-batches      │
           │                          │   to GCS (1-min trigger)           │
           │                          │ - Checkpoint for exactly-once      │
           │                          │ - Graceful shutdown on SIGTERM     │
           │                          └───────────┬────────────────────────┘
           │                                      │
           ▼                                      ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    DATA LAKE (GCS)                                        │
│  gs://at-transit/raw/gtfs_static/YYYY-MM-DD/                             │
│  gs://at-transit/processed/streaming/{topic}/yyyy/mm/dd/hh/ (Parquet)    │
└──────────────────────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│               DATA WAREHOUSE (BigQuery)                                   │
│  External Tables on GCS Parquet                                           │
│                                                                          │
│  dbt transformations:                                                     │
│    Staging: stg_gtfs_routes, stg_gtfs_stops, stg_trip_updates            │
│    Core:    dim_routes, dim_stops, fct_delay_events, fct_daily_otp       │
└──────────────────────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│               DASHBOARD (Looker Studio / Streamlit)                       │
│  Tile 1: On-Time Performance by Route (categorical bar)                   │
│  Tile 2: Delay Trend Over Time (temporal line chart)                      │
│  Tile 3 (bonus): Real-time vehicle map                                    │
└──────────────────────────────────────────────────────────────────────────┘
```

### Why no Spark Batch layer?
In the original design, Spark Batch was used for `compute_daily_otp.py` and `compute_delay_events.py`. However:
1. **dbt handles this better**: Daily OTP aggregation is a GROUP BY + COUNTIF query — BigQuery SQL via dbt is simpler, cheaper, and more maintainable than spinning up Spark for a SQL-equivalent operation
2. **Avoids redundancy with Project 1**: The NZ Electricity project already showcases Spark Batch (unpivot + clean). Repeating Spark Batch here adds no portfolio value
3. **Sharper focus**: With Spark only in streaming mode, this project tells a clearer story — "I can build production streaming pipelines"

---

## 5. Infrastructure (IaC — Terraform)

**Cloud Provider**: GCP (see Section 2 for alternatives)

| Resource                 | Purpose                                              |
| ------------------------ | ---------------------------------------------------- |
| `google_storage_bucket`  | Data lake — GCS bucket `at-transit-lake`             |
| `google_bigquery_dataset`| Data warehouse — `at_transit` dataset                |

> **Note**: Kafka and Schema Registry run locally via Docker Compose. For cloud deployment, Confluent Cloud or GCP Pub/Sub are alternatives.

---

## 6. Orchestration (Apache Airflow)

**One main DAG** (the streaming bootstrap is a manual/one-off operation, not a scheduled DAG):

### DAG: `at_gtfs_static_ingest` (Weekly, Wednesday 06:00 NZST)
| Task                  | Operator                       | Description                                    |
| --------------------- | ------------------------------ | ---------------------------------------------- |
| `check_api`           | `HttpSensor`                   | Verify AT API is reachable                     |
| `download_gtfs_zip`   | `PythonOperator`               | Download GTFS ZIP from AT GTFS API             |
| `upload_to_gcs_raw`   | `LocalFilesystemToGCSOperator` | Upload raw ZIP and extracted CSVs              |
| `load_dim_to_bq`      | `BigQueryInsertJobOperator`    | Load routes.txt, stops.txt → BQ staging tables |
| `run_dbt`             | `BashOperator`                 | Run `dbt run --select staging+ core+`          |
| `run_dbt_test`        | `BashOperator`                 | Run `dbt test`                                 |

### Streaming Bootstrap (manual / Makefile)
```bash
make kafka-up           # Start Kafka + Schema Registry via Docker Compose
make run-producer       # Start AT GTFS-RT Kafka producer
make run-streaming      # Start Spark Structured Streaming job
```

> Streaming jobs are long-running processes. Airflow manages the batch dimension table refresh; it does **not** orchestrate per-message streaming.

---

## 7. Streaming Layer (Kafka + Schema Registry + Spark Structured Streaming)

This is the **core differentiator** of this project. The streaming layer is designed with production-grade patterns.

### 7.1 Kafka Producer + Schema Registry
A Python producer script (`at_producer.py`) polls AT GTFS-RT endpoints every 30 seconds and publishes **Avro-encoded** messages to Kafka topics via Confluent Schema Registry:

```
at.trip_updates       → vehicle delay predictions per stop (Avro)
at.vehicle_positions  → GPS lat/lon per vehicle (Avro)
at.service_alerts     → disruption notices (Avro)
```

**Why Schema Registry?**
- GTFS-RT payloads are natively Protobuf; converting to Avro with a registered schema enables:
  - **Schema evolution**: Add fields without breaking downstream consumers
  - **Contract enforcement**: Producer and consumer agree on schema at topic level
  - **Production pattern**: This is how real streaming systems operate (e.g., at banks, telcos in NZ)

### 7.2 Spark Structured Streaming Job
- **Source**: Kafka topics `at.trip_updates` and `at.vehicle_positions`
- **Deserialisation**: Avro via Schema Registry (or JSON with explicit schema as fallback)
- **Processing**:
  - Parse payload into structured DataFrame
  - Enrich with `dim_routes`, `dim_stops` via **broadcast join** (loaded from BigQuery or GCS)
  - Compute `delay_seconds = actual_arrival - scheduled_arrival`
  - Add `is_late = delay_seconds > 300` (5-min threshold)
  - Apply **5-minute tumbling window** for windowed aggregation
- **Watermark**: `withWatermark("event_time", "10 minutes")` — allows late events up to 10 minutes
- **Sink**: Parquet micro-batches to `gs://at-transit/processed/streaming/trip_updates/`
  - Partitioned by `date` and `hour`
  - `trigger(processingTime="1 minute")`
- **Checkpoint**: GCS checkpoint directory for **exactly-once delivery semantics**
- **Graceful Shutdown**: Handles `SIGTERM` signal — flushes current micro-batch, commits checkpoint, then exits cleanly

### 7.3 Streaming Depth — Key Design Decisions

| Concern                  | Decision                                                          | Rationale                                          |
| ------------------------ | ----------------------------------------------------------------- | -------------------------------------------------- |
| **Exactly-once**         | Kafka → Spark checkpointing + idempotent Parquet writes           | No duplicates in data lake                         |
| **Late data**            | 10-min watermark; events beyond watermark are dropped + logged    | AT API has occasional delays; 10 min covers 99%+   |
| **Schema evolution**     | Avro + Schema Registry; BACKWARD compatible changes only          | Add fields without breaking consumers              |
| **Backpressure**         | `maxOffsetsPerTrigger` = 10000                                    | Prevent Spark OOM on Kafka lag spikes              |
| **Graceful shutdown**    | SIGTERM handler calls `query.stop()`; Docker `stop_grace_period`  | No partial/corrupt Parquet files                   |
| **Monitoring**           | Log Kafka consumer lag + batch processing time to stdout/metrics  | Detect pipeline stalls early                       |

---

## 8. Data Transformation (dbt)

dbt handles **all aggregation and modelling** — replacing the original Spark Batch jobs.

| Model              | Layer   | Materialization | Description                                                    |
| ------------------ | ------- | --------------- | -------------------------------------------------------------- |
| `stg_gtfs_routes`  | Staging | View            | Clean routes from BigQuery; map route_type to label            |
| `stg_gtfs_stops`   | Staging | View            | Clean stops with lat/lon                                       |
| `stg_trip_updates` | Staging | View            | Read streaming Parquet external table; add delay_minutes, is_late |
| `dim_routes`       | Core    | Table           | Route dimension with type (bus/train), short name              |
| `dim_stops`        | Core    | Table           | Stop dimension                                                 |
| `fct_delay_events` | Core    | Incremental     | One row per stop-time update event with delay_seconds          |
| `fct_daily_otp`    | Core    | Table           | Daily on-time performance (%) by route — **replaces Spark Batch** |

### Why dbt for daily OTP instead of Spark Batch?
```sql
-- fct_daily_otp.sql: this is a simple aggregation, no need for Spark
SELECT
    event_date,
    route_id,
    r.route_short_name,
    r.route_type_label,
    COUNT(*)                                           AS total_arrivals,
    COUNTIF(NOT is_late)                              AS on_time_arrivals,
    ROUND(COUNTIF(NOT is_late) / COUNT(*) * 100, 2)  AS otp_pct,
    ROUND(AVG(delay_minutes), 2)                      AS avg_delay_minutes
FROM {{ ref('stg_trip_updates') }} s
LEFT JOIN {{ ref('dim_routes') }} r USING (route_id)
GROUP BY 1, 2, 3, 4
```
This runs in BigQuery in seconds. Spinning up Spark for this would be over-engineering.

**dbt tests**: `not_null`, `unique`, `accepted_values` on route_type (0=tram, 2=rail, 3=bus), `dbt_expectations` range checks on otp_pct.

---

## 9. Dashboard

**Tool**: Looker Studio (connected to BigQuery) or Streamlit

| Tile               | Visual Type             | Metric                                          |
| ------------------ | ----------------------- | ----------------------------------------------- |
| **Tile 1**         | Horizontal bar chart    | On-Time Performance % by Route (Top 10 worst)   |
| **Tile 2**         | Line chart              | Average Delay (minutes) trend over last 30 days |
| **Tile 3 (bonus)** | Kepler.gl / deck.gl map | Real-time vehicle positions                     |

---

## 10. CI/CD & Engineering Standards

| Tool           | Purpose                                                   |
| -------------- | --------------------------------------------------------- |
| GitHub Actions | PR checks: Black, Ruff, SQLFluff, dbt compile, dbt test  |
| `Makefile`     | `make kafka-up`, `make run-producer`, `make run-streaming`, `make dbt-run` |
| Docker Compose | Local Kafka + Zookeeper + Schema Registry                 |
| pytest         | Unit tests for Spark streaming logic (PySpark)            |

---

## 11. Project Repository Structure

```
auckland-transport-insights/
├── terraform/                  # IaC — GCS, BigQuery
│   ├── main.tf
│   └── variables.tf
├── infra/
│   └── docker-compose.yml      # Kafka + Zookeeper + Schema Registry (local dev)
├── producers/
│   ├── at_producer.py          # Kafka producer — polls AT GTFS-RT API
│   ├── config.py               # API key + Kafka config
│   └── schemas/                # Avro schema definitions
│       ├── trip_update.avsc
│       └── vehicle_position.avsc
├── spark/
│   └── streaming/
│       ├── stream_trip_updates.py
│       └── stream_vehicle_positions.py
├── airflow/
│   └── dags/
│       └── at_gtfs_static_ingest.py   # Weekly static data refresh
├── dbt/
│   ├── dbt_project.yml
│   ├── packages.yml              # dbt-expectations
│   └── models/
│       ├── staging/
│       │   ├── stg_gtfs_routes.sql
│       │   ├── stg_gtfs_stops.sql
│       │   ├── stg_trip_updates.sql
│       │   └── schema.yml
│       └── core/
│           ├── dim_routes.sql
│           ├── dim_stops.sql
│           ├── fct_delay_events.sql
│           ├── fct_daily_otp.sql     # Replaces Spark Batch
│           └── schema.yml
├── tests/
│   └── test_streaming_logic.py
├── .github/
│   └── workflows/
│       └── ci.yml
├── Makefile
└── README.md
```

---

## 12. Technology Stack Summary

| Category              | Technology                  | Reason                                              |
| --------------------- | --------------------------- | --------------------------------------------------- |
| Cloud                 | GCP (GCS + BigQuery)        | Free tier for portfolio; swappable (see §2)         |
| IaC                   | Terraform                   | Industry standard                                   |
| Orchestration         | Apache Airflow              | Weekly batch dim refresh                            |
| Streaming Broker      | Apache Kafka (Docker)       | GTFS-RT polling → topic messages                    |
| Schema Management     | Confluent Schema Registry   | Avro schema evolution + contract enforcement        |
| Stream Processing     | Apache Spark (PySpark)      | Structured Streaming with watermarks + exactly-once |
| Data Lake             | GCS + Parquet               | Cost-effective columnar storage                     |
| Data Warehouse        | BigQuery                    | Serverless, handles daily aggregation via dbt       |
| Transformation        | dbt Core                    | Semantic modelling + replaces Spark Batch for agg   |
| Data Quality          | dbt-expectations            | Automated data validation                           |
| Dashboard             | Looker Studio               | Native BigQuery connector, free                     |
| CI/CD                 | GitHub Actions              | Automated quality gates                             |
| Language              | Python 3.11                 | Primary development language                        |

---

## 13. Portfolio Positioning: Two-Project Differentiation

| Dimension           | Project 1: NZ Electricity          | Project 2: Auckland Transport (this) |
| ------------------- | ---------------------------------- | ------------------------------------ |
| **Pipeline mode**   | Batch                              | Streaming-first                      |
| **Spark usage**     | Spark Batch (unpivot + clean)      | Spark Structured Streaming           |
| **dbt role**        | Semantic modelling + quality tests | Modelling + **daily aggregation** (replaces Spark Batch) |
| **Key depth**       | Incremental load, idempotency, DQ  | Watermark, exactly-once, Schema Registry, graceful shutdown |
| **NZ data source**  | EMI (energy sector)                | AT (public transport sector)         |
| Language                  | Python 3.11                | Primary development language         |
