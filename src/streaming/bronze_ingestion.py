import os

import requests
from dotenv import load_dotenv
from src.streaming import kafka_utils
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    base64, col, current_timestamp, expr, from_unixtime, hour,
    lit, unix_timestamp, when,
)
from pyspark.sql.functions import uuid as spark_uuid

# Producer-side event_ts sanity clamp constants (PRD Section 7a).
# MIN_PLAUSIBLE rejects epoch-zero and pre-AT-GTFS timestamps.
# Clamping here (not producer) keeps Avro schemas unchanged; semantics identical.
_MIN_PLAUSIBLE_TS = 1_600_000_000  # ~2020-09-13
_MAX_PAST_SEC = 3600               # 1 hour
_MAX_FUTURE_SEC = 300              # 5 minutes


def _ts_status(ts_col):
    """Spark column expression: classify source timestamp quality."""
    now = unix_timestamp()
    return (
        when(ts_col < _MIN_PLAUSIBLE_TS, lit("dropped_invalid"))
        .when(ts_col < now - _MAX_PAST_SEC, lit("clamped_past"))
        .when(ts_col > now + _MAX_FUTURE_SEC, lit("clamped_future"))
        .otherwise(lit("ok"))
    )


def _ts_clamped(ts_col):
    """Spark column expression: clamped unix epoch seconds."""
    now = unix_timestamp()
    return (
        when(ts_col < _MIN_PLAUSIBLE_TS, now - _MAX_PAST_SEC)
        .when(ts_col < now - _MAX_PAST_SEC, now - _MAX_PAST_SEC)
        .when(ts_col > now + _MAX_FUTURE_SEC, now + _MAX_FUTURE_SEC)
        .otherwise(ts_col)
    )


# --- enrichment functions (importable for testing) ---

def enrich_vehicle_positions(df: DataFrame) -> DataFrame:
    raw_ts = col("timestamp")
    clamped = _ts_clamped(raw_ts)
    return df.select(
        spark_uuid().alias("event_id"),
        current_timestamp().alias("ingested_at"),
        "vehicle_id", "trip_id", "route_id",
        "latitude", "longitude", "bearing",
        "speed",  # AT legacy API returns km/h directly
        "current_stop_sequence", "stop_id", "current_status",
        "congestion_level", "occupancy_status",
        from_unixtime(clamped).cast("timestamp").alias("event_ts"),
        from_unixtime(raw_ts).cast("timestamp").alias("event_ts_raw"),
        _ts_status(raw_ts).alias("event_ts_status"),
        "_raw_payload",
        from_unixtime(clamped).cast("date").alias("event_date"),
        hour(from_unixtime(clamped).cast("timestamp")).alias("event_hour"),
    )


def enrich_trip_updates(df: DataFrame) -> DataFrame:
    raw_ts = col("timestamp")
    clamped = _ts_clamped(raw_ts)
    return df.select(
        spark_uuid().alias("event_id"),
        current_timestamp().alias("ingested_at"),
        col("id").alias("source_id"),
        "trip_id", "route_id", "direction_id",
        "start_time", "start_date", "schedule_relationship",
        "delay",
        from_unixtime(clamped).cast("timestamp").alias("event_ts"),
        from_unixtime(raw_ts).cast("timestamp").alias("event_ts_raw"),
        _ts_status(raw_ts).alias("event_ts_status"),
        "is_deleted", "_raw_payload",
        from_unixtime(clamped).cast("date").alias("event_date"),
        hour(from_unixtime(clamped).cast("timestamp")).alias("event_hour"),
    )


def enrich_service_alerts(df: DataFrame) -> DataFrame:
    raw_ts = col("timestamp")
    clamped = _ts_clamped(raw_ts)
    return df.select(
        spark_uuid().alias("event_id"),
        current_timestamp().alias("ingested_at"),
        col("id").alias("alert_id"),
        "route_id", "cause", "effect", "header_text", "description_text",
        from_unixtime("active_period_start").cast("timestamp").alias("active_period_start"),
        from_unixtime("active_period_end").cast("timestamp").alias("active_period_end"),
        from_unixtime(clamped).cast("timestamp").alias("event_ts"),
        from_unixtime(raw_ts).cast("timestamp").alias("event_ts_raw"),
        _ts_status(raw_ts).alias("event_ts_status"),
        "_raw_payload",
        from_unixtime(clamped).cast("date").alias("event_date"),
        hour(from_unixtime(clamped).cast("timestamp")).alias("event_hour"),
    )


ENRICH_FNS = {
    "at.vehicle_positions": enrich_vehicle_positions,
    "at.trip_updates": enrich_trip_updates,
    "at.service_alerts": enrich_service_alerts,
}

TOPIC_CONFIG = {
    "at.vehicle_positions": {
        "key_alias": "vehicle_id",
        "flatten": [
            "data.vehicle_id", "data.trip_id", "data.route_id",
            "data.latitude", "data.longitude", "data.bearing", "data.speed",
            "data.current_stop_sequence", "data.stop_id", "data.current_status",
            "data.congestion_level", "data.occupancy_status",
            "data.timestamp",
        ],
    },
    "at.trip_updates": {
        "key_alias": "trip_id",
        "flatten": [
            "data.id", "data.trip_id", "data.route_id",
            "data.direction_id", "data.start_time", "data.start_date",
            "data.schedule_relationship", "data.delay",
            "data.timestamp", "data.is_deleted",
        ],
    },
    "at.service_alerts": {
        "key_alias": "alert_id",
        "flatten": [
            "data.id", "data.route_id",
            "data.cause", "data.effect",
            "data.header_text", "data.description_text",
            "data.active_period_start", "data.active_period_end",
            "data.timestamp",
        ],
    },
}


def start(spark: SparkSession) -> list:
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
    MAX_OFFSETS_PER_TRIGGER = int(os.getenv("BRONZE_MAX_OFFSETS_PER_TRIGGER", "5000"))
    OUTPUT_BASE = os.getenv("OUTPUT_PATH", "/tmp/bronze")
    CHECKPOINT_BASE = os.getenv("CHECKPOINT_PATH", "/tmp/checkpoints")
    OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "delta")
    TOPICS = list(TOPIC_CONFIG.keys())

    schemas = {t: kafka_utils.load_schema(t, SCHEMA_REGISTRY_URL) for t in TOPICS}

    queries = []
    for topic in TOPICS:
        table_name = topic.replace("at.", "")
        cfg = TOPIC_CONFIG[topic]

        raw = (
            spark.readStream.format("kafka")
            .options(**kafka_utils.kafka_options(KAFKA_BOOTSTRAP))
            .option("subscribe", kafka_utils.topic_name(topic))
            .option("startingOffsets", STARTING_OFFSETS)
            .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
            .load()
        )

        # Bronze is append-only raw landing — no watermark, never drops late data.
        parsed = raw.select(
            from_avro(expr(kafka_utils.AVRO_VALUE_EXPR), schemas[topic]).alias("data"),
            base64(col("value")).alias("_raw_payload"),
        )

        flat = parsed.select(
            *[col(c) for c in cfg["flatten"]],
            "_raw_payload",
        )

        enriched = ENRICH_FNS[topic](flat)

        q = (
            enriched.writeStream
            .format(OUTPUT_FORMAT)
            .option("path", f"{OUTPUT_BASE}/{table_name}")
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/{table_name}")
            .partitionBy("event_date", "event_hour")
            .outputMode("append")
            .trigger(processingTime="30 seconds")
            .queryName(table_name)
            .start()
        )
        queries.append(q)
        print(f"Started query: {table_name}")

    return queries


if __name__ == "__main__":
    load_dotenv()

    spark = (
        SparkSession.builder
        .appName("bronze_ingestion")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "org.apache.spark:spark-avro_2.12:3.4.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    from src.streaming._shutdown import run_until_shutdown
    run_until_shutdown(spark, *start(spark), job_label="bronze_ingestion")
