"""
Q1: Delay alert detection — stateless filter + Kafka fan-out.

Reads trip_updates from Kafka, filters delay > 5 min, classifies severity,
and publishes alerts to `at.alerts` topic.
"""

import os

import requests
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    col, current_timestamp, expr, from_unixtime, lit, struct, to_date, to_json,
    when,
)
from pyspark.sql.functions import uuid as spark_uuid


DELAY_THRESHOLD = 300  # 5 minutes in seconds
WATERMARK_DELAY = "10 minutes"

# --- detection logic (importable for testing) ---


def detect_delays(df: DataFrame) -> DataFrame:
    """Filter trip_updates with delay > 5 min and classify severity."""
    return (
        df.filter(col("delay") > DELAY_THRESHOLD)
        .select(
            spark_uuid().alias("event_id"),
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
    OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/tmp/bronze")
    OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "parquet")
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
    resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{SOURCE_TOPIC}-value/versions/latest", timeout=10)
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

    # flatten + watermark on event_ts (business time, not kafka ingestion time)
    flat = parsed.select(
        "data.*",
        from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_ts"),
    ).withWatermark("event_ts", WATERMARK_DELAY)

    # detect delays
    alerts = detect_delays(flat)

    # --- foreachBatch: write to Kafka + Bronze ---

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        batch_df.persist()

        try:
            # 1. Write to Kafka (at.alerts)
            kafka_ready = format_for_kafka(batch_df)
            (
                kafka_ready.write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", SINK_TOPIC)
                .save()
            )

            # 2. Write to Bronze table (bronze.delay_alerts)
            bronze_df = batch_df.withColumn(
                "event_date", to_date(col("event_timestamp"))
            )
            (
                bronze_df.write
                .format(OUTPUT_FORMAT)
                .mode("append")
                .partitionBy("event_date")
                .save(f"{OUTPUT_PATH}/delay_alerts")
            )
        finally:
            batch_df.unpersist()

    query = (
        alerts.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/delay_alerts")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .queryName("delay_alerts")
        .start()
    )

    print(f"Delay alert job started — filtering {SOURCE_TOPIC} (delay > {DELAY_THRESHOLD}s) → {SINK_TOPIC}")

    from src.streaming._shutdown import run_until_shutdown
    run_until_shutdown(spark, query, job_label="delay_alerts")
