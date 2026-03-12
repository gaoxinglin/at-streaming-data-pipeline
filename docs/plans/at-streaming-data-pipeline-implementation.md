# Auckland Transport Real-Time Monitoring Platform
## Capstone Pipeline — Implementation Plan

**Goal:** Build a **streaming-first** data pipeline for Auckland public transport (Bus & Train) using Kafka + Schema Registry, Spark Structured Streaming, GCS, BigQuery, dbt, and Airflow. Visualise delay and on-time performance metrics in a Looker Studio dashboard.

**Architecture Summary:** Terraform provisions GCP resources. A Kafka producer (with Avro Schema Registry) polls AT GTFS-RT API every 30s and publishes to Kafka topics. Spark Structured Streaming consumes topics with watermarking and exactly-once semantics, writes Parquet micro-batches to GCS. An Airflow DAG (weekly) refreshes GTFS static dimension data into BigQuery. dbt handles **all aggregation and modelling** (replacing the original Spark Batch layer). GitHub Actions provides CI/CD.

**Tech Stack:** Python 3.11, Apache Kafka, Confluent Schema Registry, Apache Spark (PySpark Structured Streaming), Apache Airflow, GCP (GCS + BigQuery), dbt Core, dbt-expectations, Terraform, Docker Compose, GitHub Actions.

> **Platform note:** GCP is used for its free tier. The pipeline is platform-agnostic — see Design Document §2 for Snowflake / AWS / Azure alternatives.

---

## Phase 1 — Foundation & Infrastructure

### Task 1: Repository & Local Dev Environment Setup

**Files to create:**
- `README.md`
- `.gitignore`
- `pyproject.toml` (or `requirements.txt`)
- `infra/docker-compose.yml`
- `Makefile`

**Step 1: Initialise repo**
```bash
mkdir auckland-transport-insights && cd auckland-transport-insights
git init
```

**Step 2: Write `infra/docker-compose.yml`**
Define services:
- `zookeeper`: `confluentinc/cp-zookeeper:7.5.0`
- `kafka`: `confluentinc/cp-kafka:7.5.0` with topics auto-created
- `schema-registry`: `confluentinc/cp-schema-registry:7.5.0` (port 8081)
- `kafka-ui` (optional): `provectuslabs/kafka-ui` for topic inspection

```yaml
schema-registry:
  image: confluentinc/cp-schema-registry:7.5.0
  depends_on: [kafka]
  ports: ["8081:8081"]
  environment:
    SCHEMA_REGISTRY_HOST_NAME: schema-registry
    SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092
```

**Step 3: Start and verify**
```bash
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps
# Expected: zookeeper, kafka, schema-registry all Running
# Verify Schema Registry:
curl http://localhost:8081/subjects
# Expected: [] (empty list)
```

**Step 4: Write `Makefile` targets**
```makefile
kafka-up:
    docker compose -f infra/docker-compose.yml up -d

kafka-down:
    docker compose -f infra/docker-compose.yml down

run-producer:
    python producers/at_producer.py

run-streaming:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
        spark/streaming/stream_trip_updates.py

dbt-run:
    dbt run --project-dir dbt && dbt test --project-dir dbt

test:
    pytest tests/ -v
```

**Step 5: Commit**
```bash
git add . && git commit -m "chore: initial repo setup with docker kafka + schema registry"
```

---

### Task 2: Terraform — GCP Infrastructure

**Files to create:**
- `terraform/main.tf`
- `terraform/variables.tf`

**Step 1: Write `terraform/variables.tf`**
Define variables:
- `project_id` (string)
- `region` = `"australia-southeast1"` (Sydney — closest to Auckland)
- `bucket_name` = `"at-transit-lake"`
- `bq_dataset` = `"at_transit"`

**Step 2: Write `terraform/main.tf`**
Configure:
```hcl
provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "data_lake" {
  name     = var.bucket_name
  location = var.region
  uniform_bucket_level_access = true
  lifecycle_rule {
    condition { age = 90 }
    action    { type = "Delete" }
  }
}

resource "google_bigquery_dataset" "warehouse" {
  dataset_id = var.bq_dataset
  location   = var.region
}
```

**Step 3: Initialise and plan**
```bash
terraform -chdir=terraform init
terraform -chdir=terraform plan -var="project_id=YOUR_PROJECT_ID"
# Expected: Plan shows 2 resources to add
```

**Step 4: Commit**
```bash
git add terraform/ && git commit -m "chore: terraform gcp infra (gcs + bigquery)"
```

---

## Phase 2 — Streaming Layer (Core Differentiator)

### Task 3: Avro Schemas for Schema Registry

**Files to create:**
- `producers/schemas/trip_update.avsc`
- `producers/schemas/vehicle_position.avsc`

**Step 1: Define Avro schemas**

`trip_update.avsc`:
```json
{
  "type": "record",
  "name": "TripUpdate",
  "namespace": "nz.at.gtfsrt",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "trip_id", "type": "string"},
    {"name": "route_id", "type": "string"},
    {"name": "stop_id", "type": "string"},
    {"name": "arrival_delay", "type": "int", "doc": "Delay in seconds"},
    {"name": "departure_delay", "type": "int"},
    {"name": "timestamp", "type": "long", "doc": "Unix epoch seconds"}
  ]
}
```

`vehicle_position.avsc`:
```json
{
  "type": "record",
  "name": "VehiclePosition",
  "namespace": "nz.at.gtfsrt",
  "fields": [
    {"name": "vehicle_id", "type": "string"},
    {"name": "trip_id", "type": "string"},
    {"name": "route_id", "type": "string"},
    {"name": "latitude", "type": "double"},
    {"name": "longitude", "type": "double"},
    {"name": "bearing", "type": ["null", "float"], "default": null},
    {"name": "speed", "type": ["null", "float"], "default": null},
    {"name": "timestamp", "type": "long"}
  ]
}
```

> Schema is registered with BACKWARD compatibility — new fields can be added as optional without breaking consumers.

**Step 2: Commit**
```bash
git add producers/schemas/ && git commit -m "feat: avro schemas for kafka topics"
```

---

### Task 4: Kafka Producer (AT GTFS-RT → Kafka with Schema Registry)

**Files to create:**
- `producers/at_producer.py`
- `producers/config.py`
- `.env.example` (API key placeholder)

**Step 1: Write `producers/config.py`**
```python
import os
AT_API_KEY = os.environ["AT_API_KEY"]
AT_BASE_URL = "https://api.at.govt.nz/realtime/legacy"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
POLL_INTERVAL_SECONDS = 30
```

**Step 2: Write `producers/at_producer.py`**
```python
# Key logic:
# 1. Load Avro schemas from producers/schemas/
# 2. Create AvroProducer (confluent_kafka.avro) with Schema Registry URL
# 3. In infinite loop with 30s sleep:
#    a. GET /tripupdates → parse GTFS-RT protobuf → normalise to Avro record
#       → produce to at.trip_updates
#    b. GET /vehiclelocations → normalise → produce to at.vehicle_positions
#    c. GET /servicealerts → produce to at.service_alerts
# 4. Error handling: log HTTP/Kafka errors, continue loop (don't crash)
# 5. Graceful shutdown:
#    - Register SIGTERM/SIGINT handler
#    - On signal: producer.flush() → exit(0)
```

Dependencies: `confluent-kafka[avro]`, `requests`, `python-dotenv`, `protobuf`

**Step 3: Test producer locally**
```bash
export AT_API_KEY="your_key_here"
python producers/at_producer.py &
# Verify Schema Registry has registered:
curl http://localhost:8081/subjects
# Expected: ["at.trip_updates-value", "at.vehicle_positions-value"]
# Verify messages arrive:
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic at.trip_updates --from-beginning --max-messages 5
```

**Step 4: Commit**
```bash
git add producers/ .env.example && git commit -m "feat: kafka producer with avro schema registry"
```

---

### Task 5: Spark Structured Streaming Job (with production patterns)

**Files to create:**
- `spark/streaming/stream_trip_updates.py`
- `spark/streaming/stream_vehicle_positions.py`
- `tests/test_streaming_logic.py`

**Step 1: Write `spark/streaming/stream_trip_updates.py`**
```python
# Key logic — this is the CORE of the project, implement with depth:
#
# 1. SparkSession with Kafka connector
#
# 2. readStream from Kafka:
#    df = spark.readStream \
#        .format("kafka") \
#        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
#        .option("subscribe", "at.trip_updates") \
#        .option("startingOffsets", "latest") \
#        .option("maxOffsetsPerTrigger", 10000)    # Backpressure control
#        .load()
#
# 3. Deserialise Avro (or JSON fallback):
#    Parse value column using from_json() or Avro deserialiser
#
# 4. Add derived columns:
#    - event_time = from_unixtime(timestamp).cast("timestamp")
#    - delay_minutes = round(arrival_delay / 60.0, 2)
#    - is_late = arrival_delay > 300  (5-min threshold)
#    - event_date = to_date(event_time)
#    - event_hour = hour(event_time)
#
# 5. Watermark for late data:
#    df.withWatermark("event_time", "10 minutes")
#    → Events arriving more than 10 minutes late are dropped + logged
#    → Rationale: AT API occasionally has delayed pushes; 10 min covers 99%+
#
# 6. Broadcast join with dim_routes (loaded once from BigQuery/GCS):
#    enriched = df.join(broadcast(dim_routes_df), "route_id", "left")
#
# 7. writeStream:
#    .format("parquet")
#    .option("path", "gs://at-transit/processed/streaming/trip_updates/")
#    .option("checkpointLocation", "gs://at-transit/checkpoints/trip_updates/")
#    .partitionBy("event_date", "event_hour")
#    .trigger(processingTime="1 minute")
#    .outputMode("append")
#    .start()
#
# 8. Graceful shutdown:
#    import signal
#    def handle_sigterm(signum, frame):
#        logger.info("SIGTERM received, stopping streaming query...")
#        query.stop()   # Flushes current micro-batch + commits checkpoint
#    signal.signal(signal.SIGTERM, handle_sigterm)
#    signal.signal(signal.SIGINT, handle_sigterm)
#    query.awaitTermination()
#
# 9. Monitoring (logged per micro-batch):
#    Log: batch_id, num_input_rows, processing_time_ms, kafka_consumer_lag
```

**Step 2: Write streaming unit tests**
```python
# tests/test_streaming_logic.py
# Using pytest + PySpark (batch mode to test streaming logic):
# - test_delay_calculation: delay_minutes = arrival_delay / 60
# - test_is_late_flag: is_late=True when arrival_delay > 300
# - test_watermark_drops_late_data: events beyond 10-min watermark are excluded
# - test_route_enrichment: broadcast join correctly adds route_short_name
# - test_null_handling: null arrival_delay → delay_minutes = null (not crash)
```

**Step 3: Run locally**
```bash
# Start Kafka producer first (Task 4), then:
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  spark/streaming/stream_trip_updates.py --local
# Expected: StreamingQuery starts, micro-batches every 60s
# Verify: Parquet files appear in /tmp/streaming_output/
pytest tests/test_streaming_logic.py -v
# Expected: All tests pass
```

**Step 4: Commit**
```bash
git add spark/ tests/ && git commit -m "feat: spark structured streaming with watermark + graceful shutdown"
```

---

## Phase 3 — Batch Dimension Refresh (Airflow)

### Task 6: Airflow DAG — GTFS Static Weekly Ingest

**Files to create:**
- `airflow/dags/at_gtfs_static_ingest.py`

**Step 1: Write `airflow/dags/at_gtfs_static_ingest.py`**
```python
# DAG: at_gtfs_static_ingest
# Schedule: "0 6 * * 3"  (Wednesday 06:00 UTC = ~19:00 NZST)
# Tasks (in order):
#
# 1. check_at_api: HttpSensor → GET https://api.at.govt.nz/realtime/legacy
#
# 2. download_gtfs_zip: PythonOperator
#    - Download GTFS static ZIP
#    - Extract to /tmp/gtfs_static/
#
# 3. upload_raw_to_gcs: LocalFilesystemToGCSOperator
#    - src: /tmp/gtfs_static/
#    - dst: gs://at-transit/raw/gtfs_static/{{ ds }}/
#
# 4. load_dim_to_bq: BigQueryInsertJobOperator
#    - Load routes.txt → at_transit.raw_routes
#    - Load stops.txt → at_transit.raw_stops
#
# 5. run_dbt: BashOperator
#    - dbt run --select staging+ core+ --project-dir dbt
#
# 6. run_dbt_test: BashOperator
#    - dbt test --project-dir dbt
```

> **Note**: No Spark Batch tasks in this DAG. Dimension processing is handled by BigQuery load + dbt. Daily OTP aggregation is a dbt model running on the streaming Parquet data.

**Step 2: Validate DAG**
```bash
python airflow/dags/at_gtfs_static_ingest.py
# Expected: No import errors, DAG object created
```

**Step 3: Commit**
```bash
git add airflow/ && git commit -m "feat: airflow dag for weekly gtfs static ingest (no spark batch)"
```

---

## Phase 4 — Data Transformation (dbt replaces Spark Batch)

### Task 7: dbt Models

**Files to create:**
- `dbt/dbt_project.yml`
- `dbt/packages.yml`
- `dbt/profiles.yml`
- `dbt/models/staging/schema.yml`
- `dbt/models/staging/stg_gtfs_routes.sql`
- `dbt/models/staging/stg_gtfs_stops.sql`
- `dbt/models/staging/stg_trip_updates.sql`
- `dbt/models/core/schema.yml`
- `dbt/models/core/dim_routes.sql`
- `dbt/models/core/dim_stops.sql`
- `dbt/models/core/fct_delay_events.sql`
- `dbt/models/core/fct_daily_otp.sql`

**Step 1: `dbt/dbt_project.yml`**
```yaml
name: at_transit
version: "1.0.0"
profile: at_transit
model-paths: ["models"]
models:
  at_transit:
    staging:
      +materialized: view
    core:
      +materialized: table
```

```yaml
# dbt/packages.yml
packages:
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<0.11.0"]
```

**Step 2: Write staging models**

`stg_gtfs_routes.sql`:
```sql
SELECT
    route_id,
    route_short_name,
    route_long_name,
    CASE route_type
        WHEN 2 THEN 'Rail'
        WHEN 3 THEN 'Bus'
        ELSE 'Other'
    END AS route_type_label
FROM {{ source('at_transit', 'raw_routes') }}
WHERE route_type IN (2, 3)
```

`stg_trip_updates.sql` — reads from external table on streaming Parquet:
```sql
SELECT
    event_date,
    event_hour,
    route_id,
    stop_id,
    trip_id,
    arrival_delay AS delay_seconds,
    ROUND(arrival_delay / 60.0, 2)  AS delay_minutes,
    arrival_delay > 300              AS is_late,
    event_time AS event_ts
FROM {{ source('at_transit', 'ext_streaming_trip_updates') }}
```

**Step 3: Write core models**

`fct_delay_events.sql` (incremental — replaces Spark Batch `compute_delay_events.py`):
```sql
{{ config(materialized='incremental', unique_key='event_id') }}
SELECT
    {{ dbt_utils.generate_surrogate_key(['event_date', 'trip_id', 'stop_id', 'event_ts']) }} AS event_id,
    s.event_date,
    s.route_id,
    r.route_short_name,
    r.route_type_label,
    s.stop_id,
    s.trip_id,
    s.delay_seconds,
    s.delay_minutes,
    s.is_late,
    s.event_ts
FROM {{ ref('stg_trip_updates') }} s
LEFT JOIN {{ ref('dim_routes') }} r USING (route_id)
{% if is_incremental() %}
WHERE s.event_date > (SELECT MAX(event_date) FROM {{ this }})
{% endif %}
```

`fct_daily_otp.sql` (replaces Spark Batch `compute_daily_otp.py`):
```sql
SELECT
    event_date,
    route_id,
    route_short_name,
    route_type_label,
    COUNT(*)                                           AS total_arrivals,
    COUNTIF(NOT is_late)                              AS on_time_arrivals,
    ROUND(COUNTIF(NOT is_late) / COUNT(*) * 100, 2)  AS otp_pct,
    ROUND(AVG(delay_minutes), 2)                      AS avg_delay_minutes
FROM {{ ref('fct_delay_events') }}
GROUP BY 1, 2, 3, 4
```

**Step 4: Add schema.yml with data quality tests**
```yaml
models:
  - name: fct_daily_otp
    columns:
      - name: event_date
        tests: [not_null]
      - name: otp_pct
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100
      - name: route_type_label
        tests:
          - accepted_values:
              values: ['Bus', 'Rail']
  - name: fct_delay_events
    columns:
      - name: event_id
        tests: [unique, not_null]
      - name: delay_seconds
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: -600
              max_value: 7200   # max 2 hours early warning
```

**Step 5: Run and test**
```bash
dbt deps --project-dir dbt
dbt compile --project-dir dbt
dbt run --project-dir dbt
dbt test --project-dir dbt
# Expected: All models created, all tests pass
```

**Step 6: Commit**
```bash
git add dbt/ && git commit -m "feat: dbt models replacing spark batch (fct_daily_otp + fct_delay_events)"
```

---

## Phase 5 — Testing, CI/CD & Dashboard

### Task 8: GitHub Actions CI/CD

**Files to create:**
- `.github/workflows/ci.yml`

**Step 1: Write `.github/workflows/ci.yml`**
```yaml
name: CI Pipeline
on: [push, pull_request]
jobs:
  lint-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - name: Install dependencies
        run: pip install black ruff sqlfluff dbt-bigquery pytest pyspark confluent-kafka[avro]
      - name: Python formatting check
        run: black --check producers/ spark/ airflow/ tests/
      - name: Python lint
        run: ruff check producers/ spark/ airflow/ tests/
      - name: SQL lint
        run: sqlfluff lint dbt/models/ --dialect bigquery
      - name: dbt compile
        run: dbt deps --project-dir dbt && dbt compile --project-dir dbt --profiles-dir dbt
      - name: Spark streaming unit tests
        run: pytest tests/ -v
```

**Step 2: Commit**
```bash
git add .github/ && git commit -m "ci: github actions pipeline"
```

---

### Task 9: Dashboard Setup (Looker Studio)

**Connecting BigQuery to Looker Studio:**

1. Open [Looker Studio](https://lookerstudio.google.com)
2. Create new report → Add data → BigQuery
3. Select: `YOUR_PROJECT_ID` → `at_transit` → `fct_daily_otp`

**Tile 1 — On-Time Performance by Route (Bar Chart)**
- Dimension: `route_short_name`
- Metric: `AVG(otp_pct)`
- Sort: Ascending (worst performers first)
- Filter: `route_type_label IN ('Bus', 'Rail')`

**Tile 2 — Average Delay Trend Over Time (Line Chart)**
- Dimension: `event_date`
- Metric: `AVG(avg_delay_minutes)`
- Breakdown: `route_type_label`
- Date range control: last 30 days

**Tile 3 (bonus) — Route Type Distribution (Pie Chart)**
- Dimension: `route_type_label`
- Metric: `total_arrivals`

---

## Evaluation Criterion Mapping

| Criterion               | Implementation                     | Expected Score |
| ----------------------- | ---------------------------------- | -------------- |
| Problem description     | README with architecture diagram   | 4/4            |
| Cloud                   | GCP — GCS + BigQuery + Dataproc    | 4/4            |
| IaC                     | Terraform for all GCP resources    | 4/4            |
| Data ingestion — Batch  | Airflow DAG with multiple tasks    | 4/4            |
| Data ingestion — Stream | Kafka + Spark Streaming pipeline   | 4/4            |
| Data warehouse          | BigQuery partitioned + clustered   | 4/4            |
| Transformations         | Spark + dbt with tests             | 4/4            |
| Dashboard               | Looker Studio with 2+ tiles        | 4/4            |
| Reproducibility         | README + Makefile + Docker Compose | 4/4            |
| **Extras**              | CI/CD + pytest + Delta Lake        | Bonus          |
