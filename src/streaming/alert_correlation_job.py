"""
Q4: Service alert correlation — multi-stream join.

When a service_alert is published, tag all live vehicle_positions on affected
routes and enrich with trip_update delay info. Chained stream-stream join:
  service_alerts ⋈ vehicle_positions ⋈ trip_updates  (on route_id + time window)

Both joins are INNER because Spark only propagates watermarks through inner
joins. A left outer join on the second leg would require the first join's
output to carry watermark info on derived columns, which Spark doesn't support.
In practice, the inner join is fine: we only emit correlations when all three
streams have matching data — which is the interesting case anyway.
"""

import os
import signal

import requests
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    col, expr, from_unixtime, struct, to_json,
)


# time window for stream-stream join — how far apart two events can be
# and still be considered related.
JOIN_WINDOW = "30 minutes"

WATERMARK_DELAY = "10 minutes"


# --- join logic (importable for testing) ---

def correlate_alerts_with_positions(
    alerts: DataFrame, positions: DataFrame,
) -> DataFrame:
    """
    Join service_alerts with vehicle_positions on route_id within a time window.

    Both DataFrames must have: route_id, event_time (timestamp).
    Watermark column (kafka_ts) is carried through for chained joins.
    """
    return (
        alerts.alias("a")
        .join(
            positions.alias("v"),
            (col("a.route_id") == col("v.route_id"))
            & (col("v.event_time").between(
                col("a.event_time") - expr(f"interval {JOIN_WINDOW}"),
                col("a.event_time") + expr(f"interval {JOIN_WINDOW}"),
            )),
            "inner",
        )
        .select(
            col("a.alert_id"),
            col("a.route_id"),
            col("a.cause"),
            col("a.effect"),
            col("a.header_text"),
            col("v.vehicle_id"),
            col("v.latitude"),
            col("v.longitude"),
            col("v.speed"),
            col("a.event_time").alias("alert_time"),
            col("v.event_time").alias("vehicle_time"),
            # carry watermarked kafka_ts through for second join
            col("v.kafka_ts").alias("kafka_ts"),
        )
    )


def enrich_with_trip_updates(
    correlated: DataFrame, trip_updates: DataFrame,
) -> DataFrame:
    """
    Enrich alert-vehicle correlations with delay info from trip_updates.

    Inner join — only emits when all three streams have matching data.
    Uses kafka_ts (watermarked) for the time-range condition so Spark
    can properly bound state and clean up old entries.
    """
    return (
        correlated.alias("c")
        .join(
            trip_updates.alias("t"),
            (col("c.route_id") == col("t.route_id"))
            & (col("t.kafka_ts").between(
                col("c.kafka_ts") - expr(f"interval {JOIN_WINDOW}"),
                col("c.kafka_ts") + expr(f"interval {JOIN_WINDOW}"),
            )),
            "inner",
        )
        .select(
            "c.alert_id",
            "c.route_id",
            "c.cause",
            "c.effect",
            "c.header_text",
            "c.vehicle_id",
            "c.latitude",
            "c.longitude",
            "c.speed",
            "c.alert_time",
            "c.vehicle_time",
            col("t.trip_id"),
            col("t.delay"),
            col("t.event_time").alias("trip_update_time"),
        )
    )


def format_for_kafka(df: DataFrame) -> DataFrame:
    return df.select(
        col("route_id").cast("string").alias("key"),
        to_json(struct("*")).alias("value"),
    )


if __name__ == "__main__":
    load_dotenv()

    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    CHECKPOINT_BASE = os.getenv("CHECKPOINT_PATH", "/tmp/checkpoints")
    SINK_TOPIC = "at.alert_correlations"

    spark = (
        SparkSession.builder
        .appName("alert_correlation")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
                "org.apache.spark:spark-avro_2.13:4.1.1")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # fetch avro schemas from SR
    topics = ["at.service_alerts", "at.vehicle_positions", "at.trip_updates"]
    schemas = {}
    for topic in topics:
        resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{topic}-value/versions/latest")
        resp.raise_for_status()
        schemas[topic] = resp.json()["schema"]

    def read_stream(topic):
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 2000)
            .load()
        )

    def parse_avro(raw, topic):
        return raw.select(
            from_avro(expr("substring(value, 6)"), schemas[topic]).alias("data"),
            col("timestamp").alias("kafka_ts"),
        )

    # --- three streams, all watermarked on kafka_ts ---
    sa_raw = parse_avro(read_stream("at.service_alerts"), "at.service_alerts")
    alerts = (
        sa_raw
        .withWatermark("kafka_ts", WATERMARK_DELAY)
        .select(
            col("data.id").alias("alert_id"),
            col("data.route_id").alias("route_id"),
            col("data.cause").alias("cause"),
            col("data.effect").alias("effect"),
            col("data.header_text").alias("header_text"),
            from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_time"),
            "kafka_ts",
        )
        .filter(col("route_id").isNotNull())
    )

    vp_raw = parse_avro(read_stream("at.vehicle_positions"), "at.vehicle_positions")
    positions = (
        vp_raw
        .withWatermark("kafka_ts", WATERMARK_DELAY)
        .select(
            col("data.vehicle_id").alias("vehicle_id"),
            col("data.route_id").alias("route_id"),
            col("data.latitude").alias("latitude"),
            col("data.longitude").alias("longitude"),
            col("data.speed").alias("speed"),
            from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_time"),
            "kafka_ts",
        )
        .filter((col("route_id").isNotNull()) & (col("route_id") != ""))
    )

    tu_raw = parse_avro(read_stream("at.trip_updates"), "at.trip_updates")
    trip_updates = (
        tu_raw
        .withWatermark("kafka_ts", WATERMARK_DELAY)
        .select(
            col("data.trip_id").alias("trip_id"),
            col("data.route_id").alias("route_id"),
            col("data.delay").alias("delay"),
            from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_time"),
            "kafka_ts",
        )
        .filter((col("route_id").isNotNull()) & (col("route_id") != ""))
    )

    # --- chained 3-stream join ---
    correlated = correlate_alerts_with_positions(alerts, positions)
    enriched = enrich_with_trip_updates(correlated, trip_updates)
    kafka_ready = format_for_kafka(enriched)

    query = (
        kafka_ready.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", SINK_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/alert_correlations")
        .outputMode("append")
        .queryName("alert_correlations")
        .start()
    )

    print(f"Alert correlation job started — 3-stream join, window={JOIN_WINDOW}")

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
                print(f"  batch {last_batch}: {rows} input rows across 3 streams")
        spark.streams.awaitAnyTermination(timeout=1)

    print("done")
