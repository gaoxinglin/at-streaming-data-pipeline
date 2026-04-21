"""
Q2: Vehicle stall detection — stateful per-vehicle processing.

Detects vehicles reporting same GPS coordinates (±15 m) for 3+ consecutive
readings within a 60-600 second event_ts span. State resets on route/trip change.
"""

import os

from dotenv import load_dotenv
from src.streaming import kafka_utils
from src.streaming.detection.stall import (
    detect_stalls, STALL_RADIUS_M, STALL_THRESHOLD,
    STALL_EVENT_SCHEMA, STATE_SCHEMA,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp, expr, from_unixtime, struct, to_date, to_json
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


def format_for_kafka(df: DataFrame) -> DataFrame:
    return df.select(
        col("vehicle_id").cast("string").alias("key"),
        to_json(struct("*")).alias("value"),
    )


def start(spark: SparkSession) -> list:
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
    MAX_OFFSETS_PER_TRIGGER = int(os.getenv("VEHICLE_POSITIONS_MAX_OFFSETS_PER_TRIGGER", "4000"))
    CHECKPOINT_BASE = os.getenv("CHECKPOINT_PATH", "/tmp/checkpoints")
    OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/tmp/bronze")
    OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "delta")
    SOURCE_TOPIC = "at.vehicle_positions"
    SINK_TOPIC = "at.alerts"

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

    # ProcessingTimeTimeout: state cleared 20 min after last reading (wall clock).
    # No watermark required; Q2 runs on on-demand cluster to avoid Spot eviction
    # causing state rebuild gaps.
    flat = parsed.select(
        col("data.vehicle_id").alias("vehicle_id"),
        col("data.route_id").alias("route_id"),
        col("data.trip_id").alias("trip_id"),
        col("data.latitude").alias("latitude"),
        col("data.longitude").alias("longitude"),
        col("data.timestamp").alias("timestamp"),
    )

    input_schema = StructType([
        StructField("vehicle_id", StringType()),
        StructField("route_id", StringType()),
        StructField("trip_id", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("timestamp", LongType()),
    ])

    stall_events = flat.groupBy("vehicle_id").applyInPandasWithState(
        detect_stalls,
        outputStructType=STALL_EVENT_SCHEMA,
        stateStructType=STATE_SCHEMA,
        outputMode="update",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
    )

    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        batch_df.persist()
        try:
            format_for_kafka(batch_df).write \
                .format("kafka") \
                .options(**kafka_utils.kafka_options(KAFKA_BOOTSTRAP)) \
                .option("topic", kafka_utils.topic_name(SINK_TOPIC)) \
                .save()

            batch_df \
                .withColumn("detected_at", current_timestamp()) \
                .withColumn("event_date", to_date(from_unixtime(col("stall_detected_ts")))) \
                .write.format(OUTPUT_FORMAT).mode("append") \
                .partitionBy("event_date") \
                .save(f"{OUTPUT_PATH}/stall_events")
        finally:
            batch_df.unpersist()

    query = (
        stall_events.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/vehicle_stalls")
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .queryName("vehicle_stalls")
        .start()
    )

    print(f"Vehicle stall detection started — "
          f"radius={STALL_RADIUS_M}m, threshold={STALL_THRESHOLD} readings")
    return [query]


if __name__ == "__main__":
    load_dotenv()

    spark = (
        SparkSession.builder
        .appName("vehicle_stall_detection")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "org.apache.spark:spark-avro_2.12:3.4.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    from src.streaming._shutdown import run_until_shutdown
    run_until_shutdown(spark, *start(spark), job_label="vehicle_stalls")
