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
[![CI](https://github.com/gaoxinglin/at-streaming-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/gaoxinglin/at-streaming-data-pipeline/actions/workflows/ci.yml)

A streaming-first data pipeline that ingests Auckland Transport GTFS-Realtime feeds, detects operational anomalies in real time using Spark Structured Streaming, and builds historical analytics with dbt on Databricks.

**Why streaming?** AT's GTFS-RT feeds are polled every 30 s during peak hours (06:00–09:00, 15:00–18:30), 60 s during shoulder hours, and 300 s overnight — with service alerts on a fixed 300 s cycle. A delay alert delivered 10 minutes late is useless to a passenger waiting at the stop; a stalled bus needs dispatch now, not in tomorrow's report. The data naturally demands streaming — this isn't batch ETL with a "streaming" label.

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

```mermaid
flowchart TD
    API["AT GTFS-RT API\nvehicle_positions · trip_updates · service_alerts\npolled every 30–300 s"]
    PROD["Python Producer  (at_producer.py)\nAvro serialization · adaptive backoff · Schema Registry"]

    subgraph KAFKA["  Kafka  ·  Redpanda (local) / Azure Event Hubs (cloud)  "]
        KIN["vehicle_positions · trip_updates · service_alerts"]
    end

    subgraph SPARK["  Spark Structured Streaming  ·  4 concurrent queries, one cluster  "]
        BRZ["Bronze Ingestion\nbronze_ingestion.py\nraw events → Parquet / Delta"]
        Q1["Q1 · Delay Alert\ndelay_alert_job.py\nstateless · delay > 5 min"]
        Q2["Q2 · Vehicle Stall\nvehicle_stall_job.py\nstateful · applyInPandasWithState"]
        Q3["Q3 · Headway Regularity\nheadway_regularity_job.py\nsliding window · bunching CV"]
    end

    KOUT["at.alerts  (Kafka topic)\ndetection fan-in from Q1 · Q2 · Q3"]
    CONS["Alerts Consumer  (alerts_consumer_job.py)\nnormalise Q1–Q3 shapes · Delta MERGE dedup"]

    subgraph STORE["  Medallion Storage  ·  Parquet (local) / Delta Lake on ADLS Gen2 (cloud)  "]
        BT["Bronze Tables\nvehicle_positions · trip_updates · service_alerts\npartitioned by event_date"]
        BA["bronze.alerts\nunified · deduplicated on alert_id"]
    end

    subgraph TRANSFORM["  dbt  ·  DuckDB (local) / Databricks SQL (cloud)  "]
        STG["Staging  (views)\nstg_vehicle_positions · stg_trip_updates\nstg_stall_events · stg_headway_metrics"]
        CORE["Core  (tables)\ndim_routes · dim_stops  (GTFS Static seeds)"]
        GOLD["Gold / Marts  (incremental)\nfct_delay_alerts · fct_delay_by_hour · fct_stall_incidents · fct_headway_regularity"]
    end

    DASH["Streamlit Dashboard  (live)  ·  Power BI  "]

    API -->|"poll"| PROD
    PROD --> KIN
    KIN --> BRZ & Q1 & Q2 & Q3
    BRZ --> BT
    Q1 & Q2 & Q3 --> KOUT
    KOUT --> CONS
    CONS --> BA
    BT & BA --> STG
    STG --> CORE & GOLD
    CORE --> GOLD
    GOLD --> DASH

    classDef source   fill:#1e40af,color:#fff,stroke:#1e3a8a
    classDef kafka    fill:#92400e,color:#fff,stroke:#78350f
    classDef spark    fill:#7c3aed,color:#fff,stroke:#6d28d9
    classDef store    fill:#b45309,color:#fff,stroke:#92400e
    classDef dbt      fill:#c2410c,color:#fff,stroke:#9a3412
    classDef dash     fill:#6b21a8,color:#fff,stroke:#581c87

    class API,PROD source
    class KIN,KOUT kafka
    class BRZ,Q1,Q2,Q3,CONS spark
    class BT,BA store
    class STG,CORE,GOLD dbt
    class DASH dash
```

### Live Dashboard Preview

Streamlit live view monitoring Q1–Q3 outputs during local runs:

![Streamlit Live Dashboard](docs/streamlit-live-dashboard.png)

Power BI dashboard connected via DirectQuery to Databricks SQL Warehouse:

![Power BI Dashboard](docs/powerbi_dashboard.png)

**Key architecture decisions:**

- **Spark for live detection, dbt for historical rollups** — no Spark batch layer. Gold-layer queries are SQL aggregations; dbt handles them in seconds on Databricks SQL.
- **Avro + Schema Registry** — contract enforcement between producer and streaming consumers. A schema-incompatible change is a build error, not a runtime surprise.
- **Medallion architecture** — Bronze (raw), Silver (cleaned views), Gold (analytics-ready incrementals).
- **Local/cloud parity** — local dev uses Redpanda + PySpark + Parquet + DuckDB; cloud uses Azure Event Hubs + Databricks + Delta Lake. The pipeline code is identical — only config changes via environment variables.
- **Single streaming cluster on Databricks** — all four Structured Streaming queries run as concurrent queries in one process on one Standard_E2ads_v6 node, cutting cluster DBU cost by ~75% vs. the previous per-job-per-cluster design (derivation below).

## Tech Stack

| Layer | Local Dev | Cloud (Azure) | Purpose |
|---|---|---|---|
| Ingestion | Python + `confluent-kafka` | Same (containerised via ACI) | Poll AT GTFS-RT APIs, serialise to Avro, publish to Kafka |
| Message broker | Redpanda (Docker) | Azure Event Hubs (Kafka-compatible) | Streaming backbone |
| Schema registry | Redpanda bundled | `fastavro` with repo-tracked `.avsc` files | Avro schema enforcement |
| Stream processing | PySpark Structured Streaming | Databricks Spark (DBR 16.4 LTS) | Bronze ingestion + Q1–Q3 detection + alerts consumer |
| Storage | Parquet files | Delta Lake on ADLS Gen2 | Medallion layers (ACID guarantees in cloud) |
| Batch transform | dbt + DuckDB | dbt + Databricks SQL | Silver/Gold layer, historical analytics |
| Orchestration | — | Databricks Workflows | Continuous streaming job + hourly dbt run |
| Infrastructure | Docker Compose | Terraform (Azure) | Local Kafka; full Azure stack provisioning |
| Dashboard | Streamlit (local) | Power BI | Live Q1–Q3 monitoring + historical BI |

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
| `at-streaming-pipeline` | Standard_E2ads_v6 spot, single-node | Continuous | `src/streaming/main.py` — all streaming queries on one cluster |
| `dbt-transform` | Standard_E2ads_v6 spot, single-node | Hourly (Auckland time) | `deploy/databricks/run_dbt.py` — seed → run → test |

**Why one cluster saves ~75%:** The previous design ran each streaming job on its own always-on cluster — four separate Standard_E2ads_v6 nodes. Databricks bills by DBU/hr; Standard_E2ads_v6 (2 vCPU, 16 GB) is 0.88 DBU/hr on Jobs Compute.

| Design | Clusters | DBU/hr |
|---|---|---|
| Old (4 jobs × own cluster) | Bronze + Q1 + Q2 + Q3, each on E2ads_v6 | 4 × 0.88 = **3.52** |
| New (1 consolidated job) | 1 × E2ads_v6 | **0.88** |
| **Saving** | | **(3.52 − 0.88) / 3.52 = 75%** |

The consolidation is safe because Spark Structured Streaming queries are independent query chains that share only the SparkSession — there is no contention between them at the query planner level.

### Cloud Infrastructure Screenshots

**Azure Resource Group**

![Resource Group Overview](docs/screenshots/azure_resource-group_overview.png)

**Event Hubs — Topics & Metrics**

<table>
<tr>
<td><img src="docs/screenshots/azure_eventhubs_topics-list.png" alt="Event Hubs Topics" width="450"/></td>
<td><img src="docs/screenshots/azure_eventhubs_namespace-metrics-3d.png" alt="Namespace Metrics (3d)" width="450"/></td>
</tr>
<tr>
<td><img src="docs/screenshots/azure_eventhubs_vehicle-positions-metrics-1d.png" alt="Vehicle Positions (1d)" width="450"/></td>
<td><img src="docs/screenshots/azure_eventhubs_vehicle-positions-metrics-1h.png" alt="Vehicle Positions (1h)" width="450"/></td>
</tr>
<tr>
<td><img src="docs/screenshots/azure_eventhubs_trip-updates-metrics.png" alt="Trip Updates Metrics" width="450"/></td>
<td><img src="docs/screenshots/azure_eventhubs_headway-metrics.png" alt="Headway Metrics" width="450"/></td>
</tr>
<tr>
<td><img src="docs/screenshots/azure_eventhubs_alerts-metrics-3d.png" alt="Alerts Metrics (3d)" width="450"/></td>
<td><img src="docs/screenshots/azure_eventhubs_service-alerts-metrics.png" alt="Service Alerts Metrics" width="450"/></td>
</tr>
</table>

**ADLS Gen2 Storage**

<table>
<tr>
<td><img src="docs/screenshots/azure_adls_containers.png" alt="ADLS Containers" width="450"/></td>
<td><img src="docs/screenshots/azure_adls_bronze-layer-tables.png" alt="Bronze Layer Tables" width="450"/></td>
</tr>
<tr>
<td colspan="2"><img src="docs/screenshots/azure_adls_bronze-vehicle-positions-partitions.png" alt="Bronze Vehicle Positions Partitions" width="450"/></td>
</tr>
</table>

**Container Registry & ACI Producer**

<table>
<tr>
<td><img src="docs/screenshots/azure_acr_at-producer-image.png" alt="ACR Producer Image" width="450"/></td>
<td><img src="docs/screenshots/azure_aci_producer-running.png" alt="ACI Producer Running" width="450"/></td>
</tr>
</table>

**Key Vault Secrets**

![Key Vault Secrets](docs/screenshots/azure_keyvault_secrets.png)

**Databricks Workflows**

<table>
<tr>
<td><img src="docs/screenshots/databricks_workflows_jobs-list.jpg" alt="Workflows Jobs List" width="450"/></td>
<td><img src="docs/screenshots/databricks_workflows_streaming-job-run-history.png" alt="Streaming Job Run History" width="450"/></td>
</tr>
</table>

**Streaming Cluster — Config & Metrics**

<table>
<tr>
<td><img src="docs/screenshots/databricks_cluster_streaming-job-config.png" alt="Streaming Job Config" width="450"/></td>
<td><img src="docs/screenshots/databricks_cluster_streaming-job-metrics.png" alt="Streaming Job Metrics" width="450"/></td>
</tr>
</table>

**Spark UI & Run Logs**

<table>
<tr>
<td><img src="docs/screenshots/databricks_spark-ui_streaming-queries.png" alt="Spark UI Streaming Queries" width="450"/></td>
<td><img src="docs/screenshots/databricks_streaming_run-logs.png" alt="Streaming Run Logs" width="450"/></td>
</tr>
</table>

**Unity Catalog — Tables**

![Catalog Tables Overview](docs/screenshots/databricks_catalog_tables-overview.png)

### Estimated cost

| State | ~NZD |
|---|---|
| Idle (no cluster running) | ~$10/month |
| Streaming cluster running (Standard_E2ads_v6 spot, 24/7) | **~$50/day** |

Fixed monthly charges at idle: Event Hubs Standard 1 TU, ACR Basic, Storage, Key Vault, Databricks workspace. The Databricks cluster dominates cost when running — pause the job when not in active use during development.

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
  │  fct_delay_alerts, fct_delay_by_hour, fct_stall_incidents, fct_headway_regularity
  │  Fact tables enriched with route/stop dimensions.
  ▼
Power BI  (DirectQuery on Databricks SQL Warehouse)
     4 report pages: Summary · Delay · Stall · Headway Regularity
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

43 unit tests across 4 files, all run with a local PySpark session — no Kafka, no Databricks, no network. The strategy isolates detection logic from the Streaming runtime: each module is tested with batch DataFrames or mocked state objects.

CI runs on every push via GitHub Actions (`.github/workflows/ci.yml`): Python lint (`black` + `ruff`), SQL lint (`sqlfluff` on dbt models), dbt compile against both DuckDB dev and Databricks prod targets, then the full unit test suite. Integration tests run on PRs to `main`. A passing badge is shown at the top of this README.

| File | Tests | What's covered |
|---|---|---|
| `test_bronze_enrichment.py` | 14 | Audit fields (`event_id`, `ingested_at`), timestamp status classification (`ok`), `event_date`/`event_hour` populated, null passthrough — VP, TU, and SA enrichment paths each covered |
| `test_vehicle_stall.py` | 12 | Haversine accuracy, stall threshold at exactly 3 readings, time span guard (< 60 s → no stall), movement reset (> 15 m), trip_id change resets state, prior state carry-over across micro-batches, timeout cleanup, ongoing stall updates |
| `test_headway_regularity.py` | 9 | Null/empty route filtered, headway mean + trip count, CV left null when < 3 trips, separate groups per route × direction, uniform headway (CV = 0.0), irregular headway flagged as bunching, start_time > 24 h (overnight service) supported |
| `test_delay_alert.py` | 8 | Threshold boundary (exactly 300 s filtered out, 301 s passes), negative delay filtered, severity tiers (MODERATE / HIGH / SEVERE), output schema, field passthrough |

Q2 (`applyInPandasWithState`) is the most complex job and also the most precisely tested: the detection function is called directly with a mocked `GroupState`, so state machine transitions are exercised without a streaming context.

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

**Bronze** — raw event tables written by Spark Structured Streaming, partitioned by `event_date`. No business logic; raw event fidelity is preserved.

**Staging** — typed dbt views over Bronze. Each view drops `_raw_payload`, casts types, and adds derived columns (`delay_minutes`, `route_type_label`). dbt `not_null` + range tests run on every column listed below.

| Model | Key columns | dbt tests |
|---|---|---|
| `stg_vehicle_positions` | `event_id` (PK), `vehicle_id`, `route_id`, `latitude`, `longitude`, `event_ts` | lat ∈ [−90, 90], lon ∈ [−180, 180] |
| `stg_trip_updates` | `event_id` (PK), `trip_id`, `route_id`, `delay` (s), `delay_minutes`, `event_ts` | not_null on all |
| `stg_stall_events` | `stall_id` (PK), `vehicle_id`, `reading_count` (≥ 3), `stall_duration_s` | reading_count ≥ 3 enforced |
| `stg_headway_metrics` | `route_id`, `trip_count` (≥ 1), `headway_mean_s`, `is_bunching` | headway_mean_s ≥ 0 |
| `stg_gtfs_routes` | `route_id` (PK), `route_short_name`, `route_type_label` | accepted_values: Bus/Rail/Other |
| `stg_gtfs_stops` | `stop_id` (PK), `stop_name`, `stop_lat`, `stop_lon` | lat ∈ [−48, −34], lon ∈ [166, 179] (NZ bounds) |

**Core** — dimension tables built from GTFS Static seeds, refreshed hourly via Databricks Workflow.

| Model | Key columns |
|---|---|
| `dim_routes` | `route_id` (PK), `route_name` (e.g. "NX1"), `route_type_label` (Bus/Rail/Other) |
| `dim_stops` | `stop_id` (PK), `stop_name`, `stop_lat`, `stop_lon` |

**Gold (Marts)** — incremental fact tables, each answering one business question. Enriched by joining staging models to `dim_routes`.

**`fct_delay_by_hour`** — *How does average delay vary across hours of the day?*

| Column | Type | Notes |
|---|---|---|
| `event_date` | date | Partition key |
| `event_hour` | int | Hour of day (0–23) |
| `avg_delay_minutes` | float | Mean delay across all trips (on-time included); captures rush-hour shape |
| `avg_delay_minutes_delayed_only` | float | Mean delay for trips with delay > 0; typically a flatter ~9 min line |
| `trip_count` | int | Total trip updates in the hour |
| `delayed_trip_count` | int | Trips with delay > 300 s |

**`fct_delay_alerts`** — *Which routes have the most delay events, and how severe?*

| Column | Type | Notes |
|---|---|---|
| `event_id` | string PK | Surrogate key from Bronze `trip_updates` |
| `event_date` | date | Partition key; derived from source event timestamp |
| `route_id` | string | FK → `dim_routes` |
| `trip_id` | string | |
| `delay` | int | Seconds; range-tested 300–7200 |
| `delay_minutes` | float | `delay / 60.0` |
| `route_type_label` | string | Bus / Rail / Other |
| `event_ts` | timestamp | Original event time |

**`fct_stall_incidents`** — *Where and when do stalls cluster? Which routes have the most incidents?*

| Column | Type | Notes |
|---|---|---|
| `stall_id` | string PK | Surrogate key from Spark Q2 detection |
| `event_date` | date | Partition key |
| `vehicle_id` | string | |
| `route_id` | string | FK → `dim_routes` |
| `stall_duration_s` | int | Approx. duration; range-tested 0–86 400 |
| `detected_at` | timestamp | Spark processing time |

**`fct_headway_regularity`** — *Which routes have the worst bunching? How does headway vary by time of day?*

| Column | Type | Notes |
|---|---|---|
| `route_id` | string | FK → `dim_routes` |
| `hour_bucket` | timestamp | Truncated to the hour |
| `avg_headway_s` | float | Mean headway across windows in the hour; ≥ 0 tested |
| `headway_cv` | float | Coefficient of variation; range-tested 0–3.0; null when < 3 trips |
| `bunching_pct` | float | % of windows where CV > 0.5; range-tested 0–100 |
| `trip_count` | int | Total trips in the hour |

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
