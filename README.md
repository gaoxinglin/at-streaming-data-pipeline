# Auckland Transport Real-time Streaming Pipeline

![Python](https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Spark_Structured_Streaming-E25A1C?logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Kafka-231F20?logo=apachekafka&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks_Workflows-FF3621?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?logo=databricks&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?logo=duckdb&logoColor=black)
![Power BI](https://img.shields.io/badge/Power_BI-F2C811?logo=powerbi&logoColor=black)
![Azure](https://img.shields.io/badge/Azure_Event_Hubs-0078D4?logo=microsoftazure&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

A streaming-first data pipeline that ingests Auckland Transport GTFS-Realtime data, detects operational anomalies in real time using Spark Structured Streaming, and builds historical analytics with dbt.

**Why streaming?** AT's GTFS-RT feeds update every 30 seconds. A delay alert delivered 10 minutes late is useless to passengers waiting at the stop; a stalled bus needs dispatch now, not in tomorrow's report. The data naturally demands streaming — this isn't batch ETL with a "streaming" label.

### Project Status

| Component                                   | Status  |
| ------------------------------------------- | ------- |
| Kafka producer (adaptive polling + Avro)    | Done    |
| Spark Bronze ingestion                      | Done    |
| Spark Q1-Q4 detection jobs                  | Done    |
| dbt project + models (staging, core, marts) | Done    |
| GTFS Static dimension tables (real AT data) | Planned |
| Databricks Workflow (weekly dim refresh)    | Planned |
| Power BI dashboard                          | Planned |

## What It Does

The pipeline answers **4 real-time operational questions**, each exercising a progressively more advanced Spark Streaming pattern:

| #   | Question                                                     | Spark Pattern                                              | Why Streaming?                                       |
| --- | ------------------------------------------------------------ | ---------------------------------------------------------- | ---------------------------------------------------- |
| Q1  | Trip delay > 5 min? Emit passenger-facing alert              | Stateless filter + Kafka fan-out                           | A delay alert delivered 10 min late is useless       |
| Q2  | Vehicle reporting same GPS (±10m) for 3+ readings?           | Stateful per-vehicle processing (`applyInPandasWithState`) | Stalled bus needs dispatch now, not tomorrow         |
| Q3  | Are buses bunching on a route? (headway CV in 10-min window) | Sliding window aggregation                                 | Dispatchers can intervene to restore spacing         |
| Q4  | Which vehicles/trips are affected by a service alert?        | Multi-stream join (3 topics)                               | Correlating disruption with impact must be real-time |

Each question also has a **historical counterpart** — dbt rolls up the streaming output into Gold fact tables for trend analysis in Power BI.

## Architecture

![Architecture Diagram](docs/architecture.png)

**Key architecture decisions:**
- **Spark for live detection, dbt for historical rollups** — no Spark Batch layer. The Gold-layer queries are SQL `GROUP BY` aggregations; dbt handles them in seconds.
- **Avro + Schema Registry** — contract enforcement between producer and streaming consumer. Production pattern used at NZ banks and telcos.
- **Medallion Architecture** — Bronze (raw), Silver (cleaned), Gold (analytics-ready).
- **Local/Cloud parity** — local dev uses Redpanda + PySpark + Parquet + DuckDB (zero-cost, `dbt run` in 1-2s); cloud uses Azure Event Hubs + Databricks + Delta Lake + Databricks SQL. The pipeline code is identical — only config changes.

## Tech Stack

| Layer             | Local Dev                    | Cloud (Azure)          | Purpose                                           |
| ----------------- | ---------------------------- | ---------------------- | ------------------------------------------------- |
| Ingestion         | Python + `confluent-kafka`   | Same                   | Poll AT APIs, serialize to Avro, publish to Kafka |
| Message Broker    | Redpanda (Docker)            | Azure Event Hubs       | Kafka-compatible streaming backbone               |
| Schema Registry   | Redpanda bundled             | Confluent-compatible   | Avro schema evolution + validation                |
| Stream Processing | PySpark Structured Streaming | Databricks Spark       | Bronze ingestion + Q1-Q4 detection jobs           |
| Storage           | Parquet                      | Delta Lake             | Medallion architecture (ACID in prod)             |
| Batch Transform   | dbt + DuckDB                 | dbt + Databricks SQL   | Silver/Gold layer, historical analytics           |
| Orchestration     | —                            | Databricks Workflows   | Weekly GTFS static refresh + dbt trigger          |
| Dashboard         | —                            | Power BI (DirectQuery) | 4 pages mapped to Q1-Q4                           |

## Project Structure

```
├── src/
│   ├── ingestion/
│   │   ├── at_producer.py          # Kafka producer with adaptive polling
│   │   └── schemas/                # Avro schemas (vehicle_position, trip_update, etc.)
│   └── streaming/
│       ├── bronze_ingestion.py     # Raw event landing (VP, TU, SA → Bronze)
│       ├── delay_alert_job.py      # Q1: stateless filter
│       ├── vehicle_stall_job.py    # Q2: stateful per-vehicle GPS tracking
│       ├── headway_regularity_job.py  # Q3: sliding window aggregation
│       ├── alert_correlation_job.py   # Q4: chained 3-stream join
│       └── _shutdown.py            # Shared graceful shutdown
├── transform/                      # dbt project
│   ├── models/
│   │   ├── staging/                # Bronze → Silver (clean views)
│   │   ├── core/                   # Dimension tables
│   │   └── marts/                  # Gold fact tables (incremental)
│   ├── dbt_project.yml
│   ├── profiles.yml                # dev: DuckDB, prod: Databricks
│   └── packages.yml
├── infrastructure/
│   └── docker-compose.yml          # Redpanda + Console (local Kafka)
├── tests/                          # pytest + PySpark unit tests
├── workflows/                      # Databricks Workflow notebooks (planned)
└── Makefile                        # Shortcuts for all common operations
```

## Quick Start

### Prerequisites

- Python 3.12+, [uv](https://github.com/astral-sh/uv) package manager
- Docker (for Redpanda)
- Java 17+ (for PySpark)
- [Auckland Transport API key](https://dev-portal.at.govt.nz/)

### Setup

```bash
# clone and install
git clone <repo-url>
cd at-streaming-data-pipeline
uv sync

# copy env and add your AT API key
cp .env.example .env

# start local Kafka (Redpanda)
make kafka-up

# start the producer (ingests AT real-time data → Kafka)
make run-producer
```

Streaming output lands in `./data/bronze/` (Parquet) with checkpoints in `./data/checkpoints/`. Both paths are configurable via `.env`.

### Run Streaming Jobs

Each job runs as a separate long-lived process. All support graceful shutdown via `Ctrl+C`.

```bash
make run-bronze   # Bronze ingestion (raw events → Parquet)
make run-q1       # Q1: delay alerts (stateless filter)
make run-q2       # Q2: vehicle stalls (stateful per-vehicle)
make run-q3       # Q3: headway regularity (windowed aggregation)
make run-q4       # Q4: alert correlation (3-stream join)
```

### Run dbt (Historical Analytics)

```bash
make dbt-deps     # install dbt packages
make dbt-seed     # load dimension seed data
make dbt-run      # build staging → core → marts
make dbt-test     # run data quality tests
```

### Run Tests

```bash
make test
```

## Streaming Job Details

### Q1: Delay Alert Detection
Filters `trip_updates` where `delay > 300s` (5 min). Classifies severity (MODERATE/HIGH/SEVERE). Writes to `at.alerts` Kafka topic + Bronze table.

### Q2: Vehicle Stall Detection
Tracks per-vehicle GPS state across micro-batches using `applyInPandasWithState`. When 3+ consecutive readings fall within a 10m Haversine radius, emits a stall event. State times out after 30 min of inactivity.

### Q3: Headway Regularity (Bus Bunching)
Computes coefficient of variation (CV) of departure headways per route in a 10-minute sliding window (2-min slide). CV > 0.5 with 3+ trips flags bunching. Deduplicates trip updates within the watermark window to prevent double-counting.

### Q4: Service Alert Correlation
Chained 3-stream join: `(service_alerts JOIN vehicle_positions) on route_id`, then `JOIN trip_updates on trip_id`. Uses differentiated watermarks (alerts=10min, positions=2min) to handle data skew. Network-wide alerts (null route_id) pass through directly without correlation.

## Data Model

**Bronze**: Raw event tables (VP, TU, SA) + detection output tables (Q1-Q4). Written by Spark Structured Streaming.

**Staging**: Cleaned views over Bronze raw tables and Bronze detection tables. Also includes `stg_gtfs_routes` / `stg_gtfs_stops` sourced from GTFS Static seed data (not Bronze).

**Core**: Dimension tables (`dim_routes`, `dim_stops`) built from GTFS Static seed data via staging.

**Gold**: Historical fact tables (incremental). Each maps to a business question:
- `fct_delay_alerts` — Q1: delay events by route over time
- `fct_stall_incidents` — Q2: stall locations and durations
- `fct_headway_regularity` — Q3: hourly headway CV by route
- `fct_alert_impact` — Q4: vehicles/trips affected per service alert

## License

This project is licensed under the [MIT License](LICENSE).
