# Auckland Transport Real-time Streaming Pipeline

![Python](https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Spark_Structured_Streaming-E25A1C?logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Kafka-231F20?logo=apachekafka&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks_Workflows-FF3621?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?logo=databricks&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?logo=duckdb&logoColor=black)
![Azure](https://img.shields.io/badge/Azure_Event_Hubs-0078D4?logo=microsoftazure&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?logo=terraform&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

A streaming-first data pipeline that ingests Auckland Transport GTFS-Realtime feeds, detects operational anomalies in real time using Spark Structured Streaming, and builds historical analytics with dbt on Databricks.

**Why streaming?** AT's GTFS-RT feeds refresh every 30 seconds. A delay alert delivered 10 minutes late is useless to a passenger waiting at the stop; a stalled bus needs dispatch now, not in tomorrow's report. The data naturally demands streaming — this isn't batch ETL with a "streaming" label.

## What It Detects

The pipeline answers three real-time operational questions, each exercising a progressively more advanced Spark Streaming pattern:

| # | Question | Spark Pattern | Why It Matters |
|---|---|---|---|
| Q1 | Trip delay > 5 min? Emit passenger-facing alert | Stateless filter + Kafka fan-out | Delay alerts are only useful in real time |
| Q2 | Vehicle reporting same GPS (±15 m) for 3+ consecutive readings? | Stateful per-vehicle tracking (`applyInPandasWithState`) | Stalled bus needs dispatch, not a next-day report |
| Q3 | Buses bunching on a route? (headway CV in a 10-min window) | Sliding window aggregation | Dispatchers can intervene to restore spacing |

Each detection job also writes its output to a unified `at.alerts` Kafka topic. An `alerts_consumer` job normalises the three message shapes and persists them to a deduplicated `bronze.alerts` Delta table using MERGE.

Every question has a **historical counterpart** — dbt rolls the streaming output into Gold fact tables for trend analysis.

## Architecture

![Architecture Diagram](docs/architecture.png)

### Live Dashboard Preview

Streamlit live view monitoring Q1–Q3 outputs during local runs:

![Streamlit Live Dashboard](docs/streamlit-live-dashboard.png)

**Key architecture decisions:**

- **Spark for live detection, dbt for historical rollups** — no Spark batch layer. Gold-layer queries are SQL aggregations; dbt handles them in seconds on Databricks SQL.
- **Avro + Schema Registry** — contract enforcement between producer and streaming consumers. A schema-incompatible change is a build error, not a runtime surprise.
- **Medallion architecture** — Bronze (raw), Silver (cleaned views), Gold (analytics-ready incrementals).
- **Local/cloud parity** — local dev uses Redpanda + PySpark + Parquet + DuckDB; cloud uses Azure Event Hubs + Databricks + Delta Lake. The pipeline code is identical — only config changes via environment variables.
- **Single streaming cluster on Databricks** — all four Structured Streaming queries run as concurrent queries in one process on one D4s_v3 node, cutting cluster cost by ~57% vs. the previous per-job-per-cluster design.

## Tech Stack

| Layer | Local Dev | Cloud (Azure) | Purpose |
|---|---|---|---|
| Ingestion | Python + `confluent-kafka` | Same (containerised via ACI) | Poll AT GTFS-RT APIs, serialise to Avro, publish to Kafka |
| Message broker | Redpanda (Docker) | Azure Event Hubs (Kafka-compatible) | Streaming backbone |
| Schema registry | Redpanda bundled | `fastavro` with repo-tracked `.avsc` files | Avro schema enforcement |
| Stream processing | PySpark Structured Streaming | Databricks Spark (DBR 15.x LTS) | Bronze ingestion + Q1–Q3 detection + alerts consumer |
| Storage | Parquet files | Delta Lake on ADLS Gen2 | Medallion layers (ACID guarantees in cloud) |
| Batch transform | dbt + DuckDB | dbt + Databricks SQL | Silver/Gold layer, historical analytics |
| Orchestration | — | Databricks Workflows | Continuous streaming job + hourly dbt run |
| Infrastructure | Docker Compose | Terraform (Azure) | Local Kafka; full Azure stack provisioning |
| Dashboard | Streamlit (local) | Power BI (planned) | Live Q1–Q3 monitoring + historical BI |

## Cloud Deployment (Azure + Databricks)

Infrastructure is fully provisioned with Terraform. A two-pass apply is required because Databricks workspace-level resources (clusters, jobs, secret scope) depend on the workspace URL.

### Azure resources provisioned

`rg-atpipe-dev`:
- ADLS Gen2 — containers: `bronze`, `silver`, `gold`, `checkpoints`, `capture`
- Azure Event Hubs namespace (Standard, Kafka-enabled) — topics: `vehicle_positions`, `trip_updates`, `service_alerts`, `alerts`, `headway_metrics`
- Azure Container Registry + ACI producer (0.25 vCPU, always-on)
- Key Vault — Event Hubs connection string, AT API key, Databricks service principal credentials
- Databricks workspace (Premium, VNet-injected to avoid NAT gateway cost)

`rg-atpipe-dev-managed` (auto-managed by Databricks):
- VNet with public/private subnets

### Databricks Workflows

Two jobs provisioned by Terraform:

| Job | Cluster | Schedule | What it runs |
|---|---|---|---|
| `at-streaming-pipeline` | D4s_v3 spot, single-node | Continuous | `src/streaming/main.py` — all streaming queries on one cluster |
| `dbt-transform` | D2s_v3 spot, single-node | Hourly (Auckland time) | `deploy/databricks/run_dbt.py` — seed → run → test |

<!-- SCREENSHOT PLACEHOLDER: Databricks Workflows tab showing both jobs with status indicators -->
<!-- Suggested: docs/databricks-workflows.png -->

<!-- SCREENSHOT PLACEHOLDER: Azure Resource Group rg-atpipe-dev overview showing all provisioned services -->
<!-- Suggested: docs/azure-resource-group.png -->

<!-- SCREENSHOT PLACEHOLDER: Azure Event Hubs namespace showing the 5 topics -->
<!-- Suggested: docs/azure-event-hubs-topics.png -->

<!-- SCREENSHOT PLACEHOLDER: ADLS Gen2 storage containers (bronze/silver/gold/checkpoints/capture) -->
<!-- Suggested: docs/azure-adls-containers.png -->

### Estimated cost (dev, idle)

| Resource | ~AUD/month |
|---|---|
| Databricks workspace (no running cluster) | ~$5 |
| Event Hubs (1 TU) | ~$13 |
| ACR Basic | ~$6 |
| ACI producer (0.25 vCPU, always-on) | ~$3 |
| Storage + Key Vault | ~$2 |
| **Total idle** | **~$29** |

Databricks cluster compute (D4s_v3 spot) adds ~$2–4/hour when running.

## Data Governance

### Data Lineage

```
AT GTFS-RT API
  │  JSON over HTTPS (polled every 30–300 s, adaptive backoff)
  ▼
Kafka Topics  (vehicle_positions / trip_updates / service_alerts)
  │  Avro — schema enforced by Schema Registry / fastavro
  ▼
Bronze Layer  (Parquet / Delta Lake)
  │  Raw event tables (VP, TU, SA) written by Spark Structured Streaming.
  │  Detection outputs (Q1–Q3) fan out to at.alerts topic, then land in
  │  bronze.alerts via the alerts_consumer job (MERGE deduplication).
  │  Partitioned by event_date. No business logic applied.
  ▼
Staging Layer  (dbt views)
  │  Type casting, null handling, column renames.
  │  stg_vehicle_positions, stg_trip_updates, stg_stall_events,
  │  stg_headway_metrics, stg_gtfs_routes, stg_gtfs_stops
  ▼
Core Layer  (dbt tables)
  │  dim_routes, dim_stops — built from GTFS Static seed data.
  ▼
Gold / Marts Layer  (dbt incremental models)
  │  fct_delay_alerts, fct_stall_incidents, fct_headway_regularity
  │  Fact tables enriched with route/stop dimensions.
  ▼
Power BI  (DirectQuery on Databricks SQL Warehouse)  [planned]
     3 report pages mapped 1:1 to Q1, Q2, Q3.
```

### Schema Contracts

Every Kafka message is serialised with **Avro** against a schema stored in Schema Registry. The `.avsc` files live in `src/ingestion/schemas/` and travel with the repo — the contract is auditable in git history. There are separate schemas for each message type: `vehicle_position`, `trip_update`, `service_alert`, and the three alert shapes (`alert_delay`, `alert_stall`, `alert_bunching`) plus a unified `alert_envelope`.

In cloud mode (Azure Event Hubs), the pipeline uses plain `fastavro` serialisation without a hosted registry. Both producer and consumer own the same `.avsc` files, so schema enforcement is maintained.

### Data Quality Controls

| Control | Where | What It Catches |
|---|---|---|
| Auckland road network bounding box | `stall.py`, `app.py` | GPS readings outside the AT service area (ocean, out-of-region) |
| Avro schema validation | Schema Registry / fastavro | Malformed messages, missing required fields |
| Stall span guard (60–600 s) | `stall.py` | GPS jitter bursts that look like stalls but resolve too fast or too slow |
| STOPPED_AT terminus filter | `stall.py` | Vehicles at scheduled stops/terminuses — not genuine stalls |
| alerts_consumer MERGE deduplication | `alerts_consumer_job.py` | At-least-once Kafka delivery producing duplicate alert rows |
| dbt `not_null` / `accepted_values` tests | `transform/tests/` | Null keys, unexpected enum values in Gold tables |
| Watermark deduplication | Q3 job | Duplicate trip_update messages within the aggregation window |

**GPS bounding box detail:** `lat -37.05 → -36.60`, `lon 174.62 → 174.95` covers Auckland's road network including the North Shore, isthmus, and South Auckland, while excluding harbour midpoints and the Hauraki Gulf.

### Audit Trail

- Every Bronze table is **partitioned by `event_date`** derived from the source event timestamp, not ingest time. Late-arriving data lands in the correct partition; historical re-runs are idempotent.
- Detection jobs stamp each emitted event with `detected_at` (processing time) alongside the original `event_ts`, preserving both event time and processing time for latency analysis.
- In cloud mode, **Delta Lake's transaction log** provides row-level ACID guarantees and full write history on the Gold layer.

## Project Structure

```
├── src/
│   ├── ingestion/
│   │   ├── at_producer.py              # Kafka producer with adaptive polling
│   │   └── schemas/                    # Avro schemas (7 message types)
│   ├── streaming/
│   │   ├── main.py                     # Cloud/Databricks entry point — all queries on one cluster
│   │   ├── bronze_ingestion.py         # Raw event landing (VP, TU, SA → Bronze)
│   │   ├── delay_alert_job.py          # Q1: stateless delay filter
│   │   ├── vehicle_stall_job.py        # Q2: stateful per-vehicle GPS tracking
│   │   ├── headway_regularity_job.py   # Q3: sliding window bus bunching
│   │   ├── alerts_consumer_job.py      # Normalises Q1–Q3 → unified bronze.alerts (MERGE)
│   │   ├── detection/
│   │   │   ├── stall.py                # GPS bounding box + stall logic (pure functions)
│   │   │   ├── delay.py                # Delay severity classification
│   │   │   └── headway.py              # Headway CV + bunching detection
│   │   └── _shutdown.py                # Shared graceful shutdown handler
│   └── dashboard/
│       └── app.py                      # Streamlit live dashboard (Q1–Q3)
├── transform/                          # dbt project
│   ├── models/
│   │   ├── staging/                    # Bronze → Silver (typed views)
│   │   ├── core/                       # Dimension tables (dim_routes, dim_stops)
│   │   └── marts/                      # Gold fact tables (incremental)
│   ├── seeds/                          # GTFS Static data (routes, stops)
│   ├── dbt_project.yml
│   └── profiles.yml                    # dev: DuckDB  |  prod: Databricks SQL
├── deploy/
│   ├── terraform/                      # Full Azure stack (Event Hubs, ADLS, Databricks, ACR, ACI, KV)
│   └── databricks/
│       └── run_dbt.py                  # dbt runner script for Databricks jobs
├── infrastructure/
│   └── docker-compose.yml              # Redpanda + Console (local Kafka)
├── tests/                              # pytest + PySpark unit tests
├── Makefile                            # Shortcuts for all common operations
└── Dockerfile                          # at-producer image (deployed to ACI via ACR)
```

## Quick Start (Local)

### Prerequisites

- Python 3.12+, [uv](https://github.com/astral-sh/uv)
- Docker (for Redpanda)
- Java 17+ (for PySpark)
- [Auckland Transport API key](https://dev-portal.at.govt.nz/)

### Setup

```bash
git clone <repo-url>
cd at-streaming-data-pipeline
uv sync

cp .env.example .env   # add your AT API key

make kafka-up          # start Redpanda (local Kafka)
make run-producer      # poll AT APIs → Kafka
```

Streaming output lands in `./data/bronze/` (Parquet) with checkpoints in `./data/checkpoints/`. Both paths are configurable via `.env`.

> **Checkpoint note:** `.env.example` defaults to `KAFKA_STARTING_OFFSETS=latest` plus low per-trigger caps so a fresh local run doesn't replay old backlog. Once a query has a checkpoint, Spark resumes from it and ignores `startingOffsets`. To replay from the beginning, stop the query, delete its checkpoint directory, and set `KAFKA_STARTING_OFFSETS=earliest`.

### Run Streaming Jobs

Each job runs as a separate long-lived process. All support graceful shutdown via `Ctrl+C`.

```bash
make run-bronze     # Bronze ingestion (raw events → Parquet)
make run-q1         # Q1: delay alerts (stateless filter)
make run-q2         # Q2: vehicle stalls (stateful per-vehicle)
make run-q3         # Q3: headway regularity (windowed aggregation)

make run-streaming  # shortcut: starts all four above
```

### Live Dashboard

```bash
streamlit run src/dashboard/app.py
```

Reads directly from Kafka. Refreshes every 30 seconds. Shows Q1–Q3 outputs including the vehicle stall density heatmap, delay severity breakdown, and headway CV by route.

### Run dbt (Historical Analytics)

```bash
make dbt-deps     # install dbt packages
make dbt-seed     # load GTFS Static dimension data
make dbt-run      # build staging → core → marts
make dbt-test     # run data quality tests
make dbt-docs     # generate + serve lineage graph
```

### Run Tests

```bash
make test
```

## Streaming Job Details

### Bronze Ingestion
Consumes all three raw Kafka topics (`vehicle_positions`, `trip_updates`, `service_alerts`) and writes them to partitioned Bronze tables. No transformation — raw event fidelity is preserved.

### Q1: Delay Alert Detection
Filters `trip_updates` where `delay > 300 s` (5 min). Classifies severity (MODERATE / HIGH / SEVERE). Publishes alerts to the `at.alerts` Kafka topic and writes to a Bronze table.

### Q2: Vehicle Stall Detection
Tracks per-vehicle GPS state across micro-batches using `applyInPandasWithState`. A stall is emitted when 3+ consecutive readings fall within a 15 m Haversine radius **and** the first-to-last span is 60–600 seconds. Readings outside the Auckland bounding box are discarded before updating state. State times out after 20 min of inactivity.

> **Known calibration gap:** Q2 can over-count stalls under certain conditions (e.g. 1000+ events in an hour). Treat spikes as a threshold tuning gap in demo logic, not a validated operational KPI.

### Q3: Headway Regularity (Bus Bunching)
Computes coefficient of variation (CV) of departure headways per route in a 10-minute sliding window (2-minute slide). CV > 0.5 with 3+ trips flags bunching. Deduplicates trip updates within the watermark window to prevent double-counting.

### Alerts Consumer
Consumes the `at.alerts` topic and normalises the three detection message shapes (Q1 delay, Q2 stall, Q3 bunching) into a unified schema. Uses Delta MERGE to deduplicate on `alert_id`, making the consumer safe under at-least-once Kafka delivery.

## Data Model

**Bronze** — raw event tables (VP, TU, SA) + unified `bronze.alerts`. Written by Spark Structured Streaming. Partitioned by `event_date`. No business logic.

**Staging** — typed views over Bronze:
`stg_vehicle_positions`, `stg_trip_updates`, `stg_stall_events`, `stg_headway_metrics`, `stg_gtfs_routes`, `stg_gtfs_stops`

**Core** — dimension tables built from GTFS Static seed data:
`dim_routes`, `dim_stops`

**Gold (Marts)** — incremental fact tables, each mapping to a business question:
- `fct_delay_alerts` — Q1: delay events with route/stop enrichment
- `fct_stall_incidents` — Q2: stall locations, durations, and vehicle IDs
- `fct_headway_regularity` — Q3: hourly headway CV by route

## Cloud Deployment (Terraform)

```bash
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars   # set owner, at_api_key, github_pat

terraform init

# Pass 1: create Databricks workspace + VNet (~10 min)
terraform apply -target=azurerm_databricks_workspace.main \
                -target=azurerm_virtual_network.databricks \
                -target=azurerm_network_security_group.databricks \
                -target=azurerm_subnet.databricks_public \
                -target=azurerm_subnet.databricks_private \
                -target=azurerm_subnet_network_security_group_association.databricks_public \
                -target=azurerm_subnet_network_security_group_association.databricks_private

# Pass 2: all remaining resources (~5 min)
terraform apply
```

After ACR is created, push the producer image:

```bash
ACR=$(terraform output -raw acr_login_server | cut -d. -f1)
az acr build --registry "$ACR" --image at-producer:latest .
```

## Scope and Non-Goals

This is a **portfolio/demo project** demonstrating end-to-end streaming architecture and implementation patterns. It is not a production-certified transit operations system.

**In scope:**
- Kafka ingestion with schema contracts (Avro)
- Spark Structured Streaming patterns — stateless, stateful (`applyInPandasWithState`), windowed aggregation
- Medallion storage architecture with dbt historical modelling
- Data governance: lineage, schema contracts, coordinate validation, data quality tests
- Full Azure cloud deployment via Terraform + Databricks Workflows
- Local/cloud parity — zero-cost local dev, identical pipeline code

**Out of scope:**
- Full calibration of detection thresholds for operational precision/recall
- Production SLA ownership for alert quality

## License

[MIT](LICENSE)
