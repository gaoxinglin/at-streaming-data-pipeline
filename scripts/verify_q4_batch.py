"""
Batch verification of Q4 alert correlation logic.

Reads Bronze parquet data (service_alerts, vehicle_positions, trip_updates),
runs the same join + aggregation logic that the streaming job uses,
and prints results. This avoids the memory cost of 3 concurrent Kafka streams.

Usage:
    PYSPARK_PYTHON=$(pwd)/.venv/bin/python .venv/bin/python scripts/verify_q4_batch.py

    # filter to a single day to reduce memory
    PYSPARK_PYTHON=$(pwd)/.venv/bin/python .venv/bin/python scripts/verify_q4_batch.py 2026-04-14
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.streaming.alert_correlation_job import (
    aggregate_for_bronze,
    correlate_alerts_with_positions,
    enrich_with_trip_updates,
)

BRONZE_PATH = "/tmp/bronze"

spark = (
    SparkSession.builder.appName("q4_batch_verify")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

date_filter = sys.argv[1] if len(sys.argv) > 1 else None

alerts = spark.read.parquet(f"{BRONZE_PATH}/service_alerts/")
positions = spark.read.parquet(f"{BRONZE_PATH}/vehicle_positions/")
trips = spark.read.parquet(f"{BRONZE_PATH}/trip_updates/")

if date_filter:
    print(f"Filtering to event_date = {date_filter}")
    alerts = alerts.filter(col("event_date") == date_filter)
    positions = positions.filter(col("event_date") == date_filter)
    trips = trips.filter(col("event_date") == date_filter)

a_count = alerts.count()
v_count = positions.count()
t_count = trips.count()
print(f"\n--- Input data ---")
print(f"service_alerts:    {a_count:,} rows")
print(f"vehicle_positions: {v_count:,} rows")
print(f"trip_updates:      {t_count:,} rows")

if a_count == 0:
    print("\nNo service_alerts found — Q4 join needs alerts to correlate against.")
    print("This is expected if AT didn't publish any alerts during collection.")
    spark.stop()
    sys.exit(0)

print("\n--- Join 1: service_alerts x vehicle_positions (route_id + 60s window) ---")
correlated = correlate_alerts_with_positions(alerts, positions)
c_count = correlated.count()
print(f"Result: {c_count:,} rows")

if c_count == 0:
    print("No correlations — alerts and positions didn't overlap within 60s window.")
    alerts.select("alert_id", "route_id", "event_ts").show(5, truncate=False)
    positions.select("vehicle_id", "route_id", "event_ts").orderBy(col("event_ts").desc()).show(5, truncate=False)
    spark.stop()
    sys.exit(0)

print("\n--- Join 2: correlated x trip_updates (trip_id + 60s window) ---")
enriched = enrich_with_trip_updates(correlated, trips)
e_count = enriched.count()
print(f"Result: {e_count:,} rows")

if e_count == 0:
    print("No enrichment matches — vehicles didn't have trip_updates within 60s.")
    correlated.select("vehicle_id", "trip_id", "event_ts").show(5, truncate=False)
    spark.stop()
    sys.exit(0)

print("\n--- Aggregated output (bronze.alert_correlations) ---")
result = aggregate_for_bronze(enriched)
r_count = result.count()
print(f"Final: {r_count:,} correlation summaries\n")
result.show(20, truncate=False)

output_path = f"{BRONZE_PATH}/alert_correlations"
result.write.format("parquet").mode("append").save(output_path)
print(f"\nWritten to {output_path}")

spark.stop()
