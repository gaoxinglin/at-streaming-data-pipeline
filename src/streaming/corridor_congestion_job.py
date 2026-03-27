"""
Q3: Live corridor congestion — sliding window aggregation.

Reads vehicle_positions from Kafka, computes average speed per route in a
5-minute sliding window (1-minute slide), and publishes congestion metrics
to `at.corridor_congestion` Kafka topic.
"""

import os
import signal

import requests
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    avg, col, count, expr, from_unixtime, lit, round as spark_round,
    struct, to_json, when, window,
)


WINDOW_DURATION = "5 minutes"
SLIDE_INTERVAL = "1 minute"
WATERMARK_DELAY = "10 minutes"


# --- detection logic (importable for testing) ---


def compute_corridor_congestion(df: DataFrame) -> DataFrame:
    """
    Aggregate avg speed per route in sliding windows.

    Input df must have: route_id, speed, event_time (timestamp type).
    Filters out records with empty route_id.
    """
    return (
        df.filter((col("route_id").isNotNull()) & (col("route_id") != ""))
        .groupBy(
            window(col("event_time"), WINDOW_DURATION, SLIDE_INTERVAL),
            "route_id",
        )
        .agg(
            spark_round(avg("speed"), 1).alias("avg_speed_kmh"),
            count("*").alias("vehicle_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "route_id",
            "avg_speed_kmh",
            "vehicle_count",
            # simple congestion classification based on avg speed
            when(col("avg_speed_kmh") < 10, lit("CONGESTED"))
            .when(col("avg_speed_kmh") < 25, lit("SLOW"))
            .otherwise(lit("FREE_FLOW"))
            .alias("congestion_level"),
        )
    )


def format_for_kafka(df: DataFrame) -> DataFrame:
    """Prepare congestion metrics for Kafka sink: key + JSON value."""
    return df.select(
        col("route_id").cast("string").alias("key"),
        to_json(struct("*")).alias("value"),
    )


if __name__ == "__main__":
    load_dotenv()

    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    CHECKPOINT_BASE = os.getenv("CHECKPOINT_PATH", "/tmp/checkpoints")
    SOURCE_TOPIC = "at.vehicle_positions"
    SINK_TOPIC = "at.corridor_congestion"

    spark = (
        SparkSession.builder
        .appName("corridor_congestion")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
                "org.apache.spark:spark-avro_2.13:4.1.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # fetch vehicle_position avro schema from SR
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

    # deserialise avro, extract fields we need
    parsed = raw.select(
        from_avro(expr("substring(value, 6)"), avro_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
    )

    flat = parsed.select(
        col("data.route_id").alias("route_id"),
        col("data.speed").alias("speed"),
        # use the vehicle's own timestamp as event time for windowing
        from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_time"),
    )

    watermarked = flat.withWatermark("event_time", WATERMARK_DELAY)

    # compute windowed aggregation
    congestion = compute_corridor_congestion(watermarked)
    kafka_ready = format_for_kafka(congestion)

    # windowed aggregation must use "update" output mode — each window's result
    # is updated as new data arrives within the window, and emitted when the
    # watermark advances past the window end.
    query = (
        kafka_ready.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", SINK_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/corridor_congestion")
        .outputMode("update")
        .queryName("corridor_congestion")
        .start()
    )

    print(f"Corridor congestion job started — "
          f"window={WINDOW_DURATION}, slide={SLIDE_INTERVAL}, watermark={WATERMARK_DELAY}")

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
                print(f"  batch {last_batch}: {rows} positions processed")
        spark.streams.awaitAnyTermination(timeout=1)

    print("done")
