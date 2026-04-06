"""
Q3: Headway regularity (bus bunching detection) — sliding window aggregation.

Reads trip_updates from Kafka, approximates departure headway by
`start_time + delay`, and computes coefficient of variation (CV)
in a 10-minute sliding window (2-minute slide).

Detects bus bunching: when buses on the same route depart in clusters
(e.g., 2-min gap then 18-min gap), even though individual trips may be on time.

Data constraint: AT's legacy endpoint provides trip-level `delay` and
`start_time` (scheduled departure from first stop), but not per-stop
arrival times. We compute departure headway, not stop-level headway.
"""

import os

import requests
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    array_sort, col, collect_list, current_timestamp, expr, from_unixtime,
    lit, size, struct, to_date, to_json, when, window,
)


WINDOW_DURATION = "10 minutes"
SLIDE_INTERVAL = "2 minutes"
WATERMARK_DELAY = "10 minutes"
BUNCHING_CV_THRESHOLD = 0.5


# --- detection logic (importable for testing) ---


def _start_time_to_seconds_expr() -> str:
    """Convert HH:MM:SS (supports 24+ hours) into seconds since midnight."""
    return (
        "cast(split(start_time, ':')[0] as int) * 3600 + "
        "cast(split(start_time, ':')[1] as int) * 60 + "
        "cast(split(start_time, ':')[2] as int)"
    )


def compute_headway_regularity(df: DataFrame) -> DataFrame:
    """
    Compute route headway CV in sliding windows.

    Input df must have: route_id, direction_id, start_time, delay, event_ts.
    We approximate actual departure seconds as start_time + delay.
    """
    enriched = (
        df.filter((col("route_id").isNotNull()) & (col("route_id") != ""))
        .filter(col("start_time").isNotNull())
        .withColumn("scheduled_departure_s", expr(_start_time_to_seconds_expr()))
        .withColumn("delay_s", expr("coalesce(delay, 0)"))
        .withColumn("actual_departure_s", col("scheduled_departure_s") + col("delay_s"))
    )

    grouped = (
        enriched
        .groupBy(
            window(col("event_ts"), WINDOW_DURATION, SLIDE_INTERVAL),
            "route_id",
            "direction_id",
        )
        .agg(
            array_sort(collect_list(col("actual_departure_s"))).alias("sorted_departures"),
        )
        .withColumn("trip_count", size(col("sorted_departures")))
    )

    with_headways = (
        grouped
        .withColumn(
            "headways",
            expr(
                "case when size(sorted_departures) >= 2 then "
                "transform(sequence(2, size(sorted_departures)), "
                "i -> element_at(sorted_departures, i) - element_at(sorted_departures, i - 1)) "
                "else array() end"
            ),
        )
        .withColumn("headway_count", size(col("headways")))
        .withColumn(
            "headway_mean_s",
            when(
                col("headway_count") > 0,
                expr("aggregate(headways, 0D, (acc, x) -> acc + x) / headway_count"),
            ),
        )
        .withColumn(
            "headway_stddev_s",
            when(
                col("headway_count") > 0,
                expr(
                    "sqrt(aggregate(headways, 0D, "
                    "(acc, x) -> acc + pow(x - headway_mean_s, 2)) / headway_count)"
                ),
            ),
        )
        .withColumn(
            "headway_cv_raw",
            when(col("headway_mean_s") > 0, col("headway_stddev_s") / col("headway_mean_s")),
        )
    )

    return (
        with_headways
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "route_id",
            "direction_id",
            "trip_count",
            "headway_mean_s",
            "headway_stddev_s",
            when(col("trip_count") >= 3, col("headway_cv_raw")).otherwise(lit(None)).alias("headway_cv"),
            when((col("trip_count") >= 3) & (col("headway_cv_raw") > BUNCHING_CV_THRESHOLD), lit(True))
            .otherwise(lit(False))
            .alias("is_bunching"),
        )
    )


def format_for_kafka(df: DataFrame) -> DataFrame:
    """Prepare headway metrics for Kafka sink: key + JSON value."""
    return df.select(
        expr("concat(route_id, ':', cast(direction_id as string))").cast("string").alias("key"),
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
    SINK_TOPIC = "at.headway_metrics"
    ALERTS_TOPIC = "at.alerts"

    spark = (
        SparkSession.builder
        .appName("headway_regularity")
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

    # deserialise avro, extract fields we need
    parsed = raw.select(
        from_avro(expr("substring(value, 6)"), avro_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
    )

    flat = parsed.select(
        col("data.trip_id").alias("trip_id"),
        col("data.route_id").alias("route_id"),
        col("data.direction_id").alias("direction_id"),
        col("data.start_time").alias("start_time"),
        col("data.delay").alias("delay"),
        from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_ts"),
    )

    watermarked = flat.withWatermark("event_ts", WATERMARK_DELAY)

    # PRD edge case: deduplicate by (trip_id, route_id, direction_id) within
    # the watermark window to prevent the same trip being counted multiple
    # times in headway calculation (AT may send updated delays for same trip).
    deduped = watermarked.dropDuplicatesWithinWatermark(
        ["trip_id", "route_id", "direction_id"]
    )

    # compute windowed aggregation
    headway_metrics = compute_headway_regularity(deduped)

    # --- foreachBatch: write to Kafka, Bronze, and alerts topic ---

    def write_batch(batch_df, batch_id):
        """Write each micro-batch to multiple sinks."""
        if batch_df.isEmpty():
            return

        batch_df.persist()

        try:
            # 1. Write all metrics to Kafka (at.headway_metrics)
            kafka_df = format_for_kafka(batch_df)
            (
                kafka_df.write
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", SINK_TOPIC)
                .save()
            )

            # 2. Write to Bronze table (bronze.headway_metrics)
            # Note: update mode may re-emit rows for the same window as new
            # data arrives. The dbt staging model handles deduplication.
            bronze_df = batch_df.withColumn("event_date", to_date(col("window_start")))
            (
                bronze_df.write
                .format(OUTPUT_FORMAT)
                .mode("append")
                .partitionBy("event_date")
                .save(f"{OUTPUT_PATH}/headway_metrics")
            )

            # 3. Write bunching alerts to at.alerts topic
            # PRD: "If CV exceeds threshold AND trip_count >= 3 → also write to at.alerts"
            bunching = batch_df.filter(col("is_bunching") == lit(True))
            if not bunching.isEmpty():
                bunching_alerts = bunching.select(
                    col("route_id").cast("string").alias("key"),
                    to_json(struct(
                        lit("bunching_alert").alias("alert_type"),
                        col("route_id"),
                        col("direction_id"),
                        col("headway_cv"),
                        col("trip_count"),
                        col("window_start"),
                        col("window_end"),
                        current_timestamp().alias("detected_at"),
                    )).alias("value"),
                )
                (
                    bunching_alerts.write
                    .format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                    .option("topic", ALERTS_TOPIC)
                    .save()
                )
        finally:
            batch_df.unpersist()

    # Windowed aggregation uses "update" output mode — each window's result
    # is updated as new data arrives, emitted when the watermark advances.
    # foreachBatch enables writing to multiple sinks per micro-batch.
    query = (
        headway_metrics.writeStream
        .outputMode("update")
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/headway_metrics")
        .queryName("headway_metrics")
        .start()
    )

    print(f"Headway regularity job started — "
          f"window={WINDOW_DURATION}, slide={SLIDE_INTERVAL}, watermark={WATERMARK_DELAY}")

    from src.streaming._shutdown import run_until_shutdown
    run_until_shutdown(spark, query, job_label="headway_regularity")
