"""
Q1: Delay alert detection — stateless filter + Kafka fan-out.

Reads trip_updates from Kafka, filters delay > 5 min, classifies severity,
and publishes alerts to `at.alerts` topic.
"""

import os
import signal

import requests
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    col, current_timestamp, expr, lit, struct, to_json,
    unix_timestamp, when,
)
from pyspark.sql.functions import uuid as spark_uuid


DELAY_THRESHOLD = 300  # 5 minutes in seconds

# --- detection logic (importable for testing) ---


def detect_delays(df: DataFrame) -> DataFrame:
    """Filter trip_updates with delay > 5 min and classify severity."""
    return (
        df.filter(col("delay") > DELAY_THRESHOLD)
        .select(
            spark_uuid().alias("alert_id"),
            "trip_id",
            "route_id",
            "delay",
            when(col("delay") <= 600, lit("MODERATE"))     # 5-10 min
            .when(col("delay") <= 1200, lit("HIGH"))       # 10-20 min
            .otherwise(lit("SEVERE"))                      # 20+ min
            .alias("severity"),
            "start_time",
            "start_date",
            col("timestamp").alias("event_timestamp"),
            current_timestamp().alias("detected_at"),
        )
    )


def format_for_kafka(df: DataFrame) -> DataFrame:
    """Prepare alert DataFrame for Kafka sink: key + JSON value."""
    return df.select(
        col("trip_id").cast("string").alias("key"),
        to_json(struct("*")).alias("value"),
    )


if __name__ == "__main__":
    load_dotenv()

    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    CHECKPOINT_BASE = os.getenv("CHECKPOINT_PATH", "/tmp/checkpoints")
    SOURCE_TOPIC = "at.trip_updates"
    SINK_TOPIC = "at.alerts"

    spark = (
        SparkSession.builder
        .appName("delay_alert_detection")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
                "org.apache.spark:spark-avro_2.13:4.1.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # fetch trip_update avro schema from SR
    resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{SOURCE_TOPIC}-value/versions/latest")
    resp.raise_for_status()
    avro_schema = resp.json()["schema"]

    # read from kafka
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )

    # deserialise avro — skip 5-byte Confluent SR header
    parsed = raw.select(
        from_avro(expr("substring(value, 6)"), avro_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
    )

    # flatten + watermark
    flat = (
        parsed
        .withWatermark("kafka_timestamp", "10 minutes")
        .select("data.*")
    )

    # detect delays + format for kafka sink
    alerts = detect_delays(flat)
    kafka_ready = format_for_kafka(alerts)

    # write to at.alerts topic
    query = (
        kafka_ready.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", SINK_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/delay_alerts")
        .outputMode("append")
        .queryName("delay_alerts")
        .start()
    )

    print(f"Delay alert job started — filtering {SOURCE_TOPIC} (delay > {DELAY_THRESHOLD}s) → {SINK_TOPIC}")

    # graceful shutdown
    _shutdown = False

    def _stop(sig, frame):
        global _shutdown
        print(f"\nCaught signal {sig}, shutting down...")
        _shutdown = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    last_batch = -1
    while query.isActive:
        if _shutdown:
            query.stop()
            break
        progress = query.lastProgress
        if progress and progress.get("batchId", -1) > last_batch:
            last_batch = progress["batchId"]
            rows = progress.get("numInputRows", 0)
            if rows > 0:
                print(f"  batch {last_batch}: {rows} trip_updates scanned")
        spark.streams.awaitAnyTermination(timeout=1)

    print("done")
