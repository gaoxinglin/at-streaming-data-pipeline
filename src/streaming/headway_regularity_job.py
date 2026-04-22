"""
Q3: Headway regularity (bus bunching detection) — sliding window aggregation.

Current implementation: departure-headway approximation from trip_updates
(start_time + delay). PRD v5.1 calls for VP-based stop-arrival headway;
migration pending stop_times.txt ingestion (Phase 3).
"""

import os

from dotenv import load_dotenv
from src.streaming import kafka_utils
from src.streaming.detection.headway import (
    compute_headway_regularity,
    WINDOW_DURATION,
    SLIDE_INTERVAL,
    WATERMARK_DELAY,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    col,
    current_timestamp,
    expr,
    from_unixtime,
    lit,
    struct,
    to_date,
    to_json,
    when,
)


def format_for_kafka(df: DataFrame) -> DataFrame:
    return df.select(
        expr("concat(route_id, ':', cast(direction_id as string))")
        .cast("string")
        .alias("key"),
        to_json(struct("*")).alias("value"),
    )


def start(spark: SparkSession) -> list:
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
    MAX_OFFSETS_PER_TRIGGER = int(
        os.getenv("TRIP_UPDATES_MAX_OFFSETS_PER_TRIGGER", "2000")
    )
    CHECKPOINT_BASE = os.getenv("CHECKPOINT_PATH", "/tmp/checkpoints")
    OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/tmp/bronze")
    OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "delta")
    SOURCE_TOPIC = "at.trip_updates"
    SINK_TOPIC = "at.headway_metrics"
    ALERTS_TOPIC = "at.alerts"

    avro_schema = kafka_utils.load_schema(SOURCE_TOPIC, SCHEMA_REGISTRY_URL)

    raw = (
        spark.readStream.format("kafka")
        .options(**kafka_utils.kafka_options(KAFKA_BOOTSTRAP))
        .option("subscribe", kafka_utils.topic_name(SOURCE_TOPIC))
        .option("startingOffsets", STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    parsed = raw.select(
        from_avro(expr(kafka_utils.AVRO_VALUE_EXPR), avro_schema).alias("data"),
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

    # Dedup within watermark to avoid the same trip update being counted
    # multiple times in headway calculation (AT may push updated delays).
    deduped = watermarked.dropDuplicatesWithinWatermark(
        ["trip_id", "route_id", "direction_id"]
    )

    headway_metrics = compute_headway_regularity(deduped)

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        batch_df.persist()
        try:
            format_for_kafka(batch_df).write.format("kafka").options(
                **kafka_utils.kafka_options(KAFKA_BOOTSTRAP)
            ).option("topic", kafka_utils.topic_name(SINK_TOPIC)).save()

            batch_df.withColumn(
                "event_date", to_date(col("window_start"))
            ).write.format(OUTPUT_FORMAT).mode("append").partitionBy("event_date").save(
                f"{OUTPUT_PATH}/headway_metrics"
            )

            bunching = batch_df.filter(col("is_bunching") == lit(True))
            if not bunching.isEmpty():
                bunching.select(
                    col("route_id").cast("string").alias("key"),
                    to_json(
                        struct(
                            lit("bunching_alert").alias("alert_type"),
                            col("route_id"),
                            col("direction_id"),
                            col("headway_cv"),
                            col("trip_count"),
                            col("window_start"),
                            col("window_end"),
                            current_timestamp().alias("detected_at"),
                        )
                    ).alias("value"),
                ).write.format("kafka").options(
                    **kafka_utils.kafka_options(KAFKA_BOOTSTRAP)
                ).option(
                    "topic", kafka_utils.topic_name(ALERTS_TOPIC)
                ).save()
        finally:
            batch_df.unpersist()

    query = (
        headway_metrics.writeStream.outputMode("update")
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/headway_metrics")
        .trigger(processingTime="30 seconds")
        .queryName("headway_metrics")
        .start()
    )

    print(
        f"Headway regularity job started — "
        f"window={WINDOW_DURATION}, slide={SLIDE_INTERVAL}, watermark={WATERMARK_DELAY}"
    )
    return [query]


if __name__ == "__main__":
    load_dotenv()

    spark = (
        SparkSession.builder.appName("headway_regularity")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "org.apache.spark:spark-avro_2.12:3.4.1",
        )
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"),
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    from src.streaming._shutdown import run_until_shutdown

    run_until_shutdown(spark, *start(spark), job_label="headway_regularity")
