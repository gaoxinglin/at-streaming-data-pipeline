"""
Q1: Delay alert detection — stateless filter + Kafka fan-out.

Reads trip_updates from Kafka, filters delay > 5 min, classifies severity,
and publishes alerts to `at.alerts` topic.
"""

import os

from dotenv import load_dotenv
from src.streaming import kafka_utils
from src.streaming.detection.delay import detect_delays, DELAY_THRESHOLD
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, from_unixtime, struct, to_date, to_json


def format_for_kafka(df: DataFrame) -> DataFrame:
    return df.select(
        col("trip_id").cast("string").alias("key"),
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
    SINK_TOPIC = "at.alerts"
    WATERMARK_DELAY = "5 minutes"

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
        "data.*",
        from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_ts"),
    ).withWatermark("event_ts", WATERMARK_DELAY)

    alerts = detect_delays(flat)

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        batch_df.persist()
        try:
            format_for_kafka(batch_df).write.format("kafka").options(
                **kafka_utils.kafka_options(KAFKA_BOOTSTRAP)
            ).option("topic", kafka_utils.topic_name(SINK_TOPIC)).save()

            batch_df.withColumn(
                "event_date", to_date(from_unixtime(col("event_timestamp")))
            ).write.format(OUTPUT_FORMAT).mode("append").partitionBy("event_date").save(
                f"{OUTPUT_PATH}/delay_alerts"
            )
        finally:
            batch_df.unpersist()

    query = (
        alerts.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/delay_alerts")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .queryName("delay_alerts")
        .start()
    )

    print(
        f"Delay alert job started — filtering {SOURCE_TOPIC} (delay > {DELAY_THRESHOLD}s) → {SINK_TOPIC}"
    )
    return [query]


if __name__ == "__main__":
    load_dotenv()

    spark = (
        SparkSession.builder.appName("delay_alert_detection")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "org.apache.spark:spark-avro_2.12:3.4.1",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    from src.streaming._shutdown import run_until_shutdown

    run_until_shutdown(spark, *start(spark), job_label="delay_alerts")
