import os

import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import base64, col, current_timestamp, expr, from_unixtime
from pyspark.sql.functions import uuid as spark_uuid


# --- enrichment functions (importable for testing) ---

def enrich_vehicle_positions(df: DataFrame) -> DataFrame:
    return df.select(
        spark_uuid().alias("event_id"),
        current_timestamp().alias("ingestion_timestamp"),
        "vehicle_id",
        "trip_id",
        "route_id",
        "latitude",
        "longitude",
        "bearing",
        "speed",  # AT legacy API returns km/h directly
        "current_stop_sequence",
        "stop_id",
        "current_status",
        from_unixtime("timestamp").cast("timestamp").alias("timestamp"),
        "congestion_level",
        "occupancy_status",
        "_raw_payload",
        from_unixtime("timestamp").cast("date").alias("event_date"),
    )


def enrich_trip_updates(df: DataFrame) -> DataFrame:
    return df.select(
        spark_uuid().alias("event_id"),
        current_timestamp().alias("ingestion_timestamp"),
        col("id").alias("source_id"),
        "trip_id",
        "route_id",
        "direction_id",
        "start_time",
        "start_date",
        "schedule_relationship",
        "delay",
        from_unixtime("timestamp").cast("timestamp").alias("timestamp"),
        "is_deleted",
        "_raw_payload",
        from_unixtime("timestamp").cast("date").alias("event_date"),
    )


def enrich_service_alerts(df: DataFrame) -> DataFrame:
    return df.select(
        spark_uuid().alias("event_id"),
        current_timestamp().alias("ingestion_timestamp"),
        col("id").alias("alert_id"),
        "route_id",
        "cause",
        "effect",
        "header_text",
        "description_text",
        from_unixtime("active_period_start").cast("timestamp").alias("active_period_start"),
        from_unixtime("active_period_end").cast("timestamp").alias("active_period_end"),
        from_unixtime("timestamp").cast("timestamp").alias("timestamp"),
        "_raw_payload",
        from_unixtime("timestamp").cast("date").alias("event_date"),
    )


ENRICH_FNS = {
    "at.vehicle_positions": enrich_vehicle_positions,
    "at.trip_updates": enrich_trip_updates,
    "at.service_alerts": enrich_service_alerts,
}

# topic → (key column in avro, columns to flatten from data.*)
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


if __name__ == "__main__":
    load_dotenv()

    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    OUTPUT_BASE = os.getenv("BRONZE_OUTPUT_PATH", "/tmp/bronze")
    CHECKPOINT_BASE = os.getenv("CHECKPOINT_PATH", "/tmp/checkpoints")
    OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "parquet")
    TOPICS = list(TOPIC_CONFIG.keys())

    spark = (
        SparkSession.builder
        .appName("bronze_ingestion")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
                "org.apache.spark:spark-avro_2.13:4.1.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # fetch schemas from SR
    schemas = {}
    for topic in TOPICS:
        resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{topic}-value/versions/latest", timeout=10)
        resp.raise_for_status()
        schemas[topic] = resp.json()["schema"]

    # start one streaming query per topic
    queries = {}
    for topic in TOPICS:
        table_name = topic.replace("at.", "")  # vehicle_positions, trip_updates, service_alerts
        cfg = TOPIC_CONFIG[topic]

        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 10000)
            .load()
        )

        # deserialise avro, keep raw payload as base64 for debugging
        # No watermark here — Bronze is append-only raw landing, never drops late data.
        parsed = raw.select(
            from_avro(expr("substring(value, 6)"), schemas[topic]).alias("data"),
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
            .partitionBy("event_date")
            .outputMode("append")
            .trigger(processingTime="30 seconds")
            .queryName(table_name)
            .start()
        )
        queries[table_name] = q
        print(f"Started query: {table_name}")

    from src.streaming._shutdown import run_until_shutdown
    run_until_shutdown(spark, *queries.values(), job_label="bronze_ingestion")
