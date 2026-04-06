"""
Q2: Vehicle stall detection — stateful per-vehicle processing.

Detects vehicles reporting same GPS coordinates (±10m) for 3+ consecutive
readings using applyInPandasWithState.
"""

import math
import os
from typing import Iterator

import pandas as pd
import requests
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    col, current_timestamp, expr, from_unixtime, struct, to_date, to_json,
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
    TimestampType,
)


STALL_THRESHOLD = 3      # consecutive readings within radius
STALL_RADIUS_M = 10.0    # metres
STATE_TIMEOUT = "30 minutes"

# schema for the stall events we emit
STALL_EVENT_SCHEMA = StructType([
    StructField("vehicle_id", StringType()),
    StructField("route_id", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("consecutive_count", IntegerType()),
    StructField("stall_start_ts", LongType()),
    StructField("stall_detected_ts", LongType()),
])

# schema for per-vehicle state stored between micro-batches
STATE_SCHEMA = StructType([
    StructField("anchor_lat", DoubleType()),
    StructField("anchor_lon", DoubleType()),
    StructField("count", IntegerType()),
    StructField("first_ts", LongType()),
    StructField("last_route_id", StringType()),
    StructField("already_emitted", IntegerType()),  # bool as int for pandas compat
])


# --- haversine (importable for testing) ---

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres between two GPS points."""
    R = 6_371_000  # earth radius in metres
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# --- stateful processing function ---

def detect_stalls(
    key: tuple,
    readings: Iterator[pd.DataFrame],
    state: GroupState,
) -> Iterator[pd.DataFrame]:
    """
    Per-vehicle stateful stall detection.

    State tracks: anchor position, consecutive count, first timestamp.
    Emits a stall event when count reaches STALL_THRESHOLD, and again
    each time count increments beyond that (ongoing stall).
    """
    if state.hasTimedOut:
        state.remove()
        return iter([pd.DataFrame(columns=[f.name for f in STALL_EVENT_SCHEMA.fields])])

    # load existing state or initialise
    if state.exists:
        s = state.get
        anchor_lat = float(s[0])
        anchor_lon = float(s[1])
        count = int(s[2])
        first_ts = int(s[3])
        last_route = str(s[4])
        already_emitted = int(s[5])
    else:
        anchor_lat = anchor_lon = 0.0
        count = 0
        first_ts = 0
        last_route = ""
        already_emitted = 0

    events = []

    for batch in readings:
        # sort by timestamp within this micro-batch
        batch = batch.sort_values("timestamp")
        for _, row in batch.iterrows():
            lat, lon = float(row["latitude"]), float(row["longitude"])
            ts = int(row["timestamp"])
            route = str(row["route_id"]) if pd.notna(row["route_id"]) else ""

            if count == 0:
                # first reading — set anchor
                anchor_lat, anchor_lon = lat, lon
                count = 1
                first_ts = ts
                last_route = route
            elif haversine_m(anchor_lat, anchor_lon, lat, lon) <= STALL_RADIUS_M:
                count += 1
                last_route = route or last_route
            else:
                # moved — reset anchor
                anchor_lat, anchor_lon = lat, lon
                count = 1
                first_ts = ts
                last_route = route
                already_emitted = 0

            # emit stall event when threshold reached or ongoing stall grows
            if count >= STALL_THRESHOLD and count > already_emitted:
                events.append({
                    "vehicle_id": key[0],
                    "route_id": last_route,
                    "latitude": anchor_lat,
                    "longitude": anchor_lon,
                    "consecutive_count": count,
                    "stall_start_ts": first_ts,
                    "stall_detected_ts": ts,
                })
                already_emitted = count

    # save state
    state.update((anchor_lat, anchor_lon, count, first_ts, last_route, already_emitted))
    state.setTimeoutDuration(30 * 60 * 1000)  # 30 minutes in ms

    if events:
        return iter([pd.DataFrame(events)])
    else:
        return iter([pd.DataFrame(columns=[f.name for f in STALL_EVENT_SCHEMA.fields])])


def format_for_kafka(df: DataFrame) -> DataFrame:
    """Prepare stall events for Kafka sink."""
    return df.select(
        col("vehicle_id").cast("string").alias("key"),
        to_json(struct("*")).alias("value"),
    )


if __name__ == "__main__":
    load_dotenv()

    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    CHECKPOINT_BASE = os.getenv("CHECKPOINT_PATH", "/tmp/checkpoints")
    OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/tmp/bronze")
    OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "parquet")
    SOURCE_TOPIC = "at.vehicle_positions"
    SINK_TOPIC = "at.alerts"

    spark = (
        SparkSession.builder
        .appName("vehicle_stall_detection")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
                "org.apache.spark:spark-avro_2.13:4.1.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # fetch avro schema from SR
    resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{SOURCE_TOPIC}-value/versions/latest", timeout=10)
    resp.raise_for_status()
    avro_schema = resp.json()["schema"]

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )

    parsed = raw.select(
        from_avro(expr("substring(value, 6)"), avro_schema).alias("data"),
    )

    # No watermark — applyInPandasWithState uses ProcessingTimeTimeout,
    # which doesn't depend on event-time watermark. State is cleared when
    # a vehicle stops reporting for 30 min (wall clock).
    flat = parsed.select(
        col("data.vehicle_id").alias("vehicle_id"),
        col("data.route_id").alias("route_id"),
        col("data.latitude").alias("latitude"),
        col("data.longitude").alias("longitude"),
        col("data.timestamp").alias("timestamp"),
    )

    # input schema for applyInPandasWithState (must match flat columns minus grouping key)
    input_schema = StructType([
        StructField("vehicle_id", StringType()),
        StructField("route_id", StringType()),
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

            # 2. Write to Bronze table (bronze.stall_events)
            bronze_df = batch_df.withColumn(
                "detected_at", current_timestamp()
            ).withColumn(
                "event_date", to_date(from_unixtime(col("stall_detected_ts")))
            )
            (
                bronze_df.write
                .format(OUTPUT_FORMAT)
                .mode("append")
                .partitionBy("event_date")
                .save(f"{OUTPUT_PATH}/stall_events")
            )
        finally:
            batch_df.unpersist()

    query = (
        stall_events.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/vehicle_stalls")
        .outputMode("update")
        .queryName("vehicle_stalls")
        .start()
    )

    print(f"Vehicle stall detection started — "
          f"radius={STALL_RADIUS_M}m, threshold={STALL_THRESHOLD} readings, timeout={STATE_TIMEOUT}")

    from src.streaming._shutdown import run_until_shutdown
    run_until_shutdown(spark, query, job_label="vehicle_stalls")
