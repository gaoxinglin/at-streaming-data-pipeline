"""
Q4: Service alert correlation — multi-stream join.

When a service_alert is published, tag all live vehicle_positions on affected
routes and enrich with trip_update delay info. Chained stream-stream join:
  (service_alerts ⋈ vehicle_positions) on route_id
  then ⋈ trip_updates on trip_id  (vehicle's actual trip)

Both joins are INNER because Spark only propagates watermarks through inner
joins. In practice, the inner join is fine: we only emit correlations when
all three streams have matching data — which is the interesting case anyway.

Network-wide alerts (route_id = NULL) are passed through directly to the
alerts topic without vehicle/trip correlation (PRD edge case).
"""

import os

import requests
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    avg, col, countDistinct, current_timestamp, expr, from_unixtime, lit,
    struct, to_date, to_json,
)
from pyspark.sql.functions import uuid as spark_uuid


# PRD §Threshold Rationale: Q4 temporal window = 60s.
# Matches Q1's latency SLA; alert correlation should happen within the same cycle.
JOIN_WINDOW = "60 seconds"

# PRD §Edge Cases: Differentiated watermarks to handle data skew.
# Service alerts are sparse (~tens/day) — watermark advances slowly.
# Vehicle positions are dense (~thousands/min) — tighter watermark prevents
# unbounded state accumulation.
WATERMARK_ALERTS = "10 minutes"
WATERMARK_POSITIONS = "2 minutes"
WATERMARK_TRIP_UPDATES = "10 minutes"


# --- join logic (importable for testing) ---

def correlate_alerts_with_positions(
    alerts: DataFrame, positions: DataFrame,
) -> DataFrame:
    """
    Join service_alerts with vehicle_positions on route_id within a time window.

    Both DataFrames must have: route_id, event_ts (timestamp).
    Positions must also have trip_id (carried through for the second join
    so it can narrow on trip_id instead of route_id, avoiding cartesian blow-up).
    """
    return (
        alerts.alias("a")
        .join(
            positions.alias("v"),
            (col("a.route_id") == col("v.route_id"))
            & (col("v.event_ts").between(
                col("a.event_ts") - expr(f"interval {JOIN_WINDOW}"),
                col("a.event_ts") + expr(f"interval {JOIN_WINDOW}"),
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
            col("v.trip_id"),
            col("v.latitude"),
            col("v.longitude"),
            col("v.speed"),
            # Materialize display timestamps without watermark metadata.
            # A no-op cast(timestamp->timestamp) can preserve event-time markers,
            # so we force a non-noop roundtrip via epoch seconds.
            from_unixtime(col("a.event_ts").cast("long")).cast("timestamp").alias("alert_time"),
            from_unixtime(col("v.event_ts").cast("long")).cast("timestamp").alias("vehicle_time"),
            col("v.event_ts").alias("event_ts"),
        )
    )


def enrich_with_trip_updates(
    correlated: DataFrame, trip_updates: DataFrame,
) -> DataFrame:
    """
    Enrich alert-vehicle correlations with delay info from trip_updates.

    Inner join on trip_id (not route_id) — each vehicle is on exactly one
    trip at a time, so this gives 1:1 enrichment instead of the N×M
    cartesian that route_id would produce. Still uses event_ts time-range
    condition so Spark can bound state and clean up old entries.
    """
    return (
        correlated.alias("c")
        .join(
            trip_updates.alias("t"),
            (col("c.trip_id") == col("t.trip_id"))
            & (col("t.event_ts").between(
                col("c.event_ts") - expr(f"interval {JOIN_WINDOW}"),
                col("c.event_ts") + expr(f"interval {JOIN_WINDOW}"),
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
            "c.trip_id",
            "c.latitude",
            "c.longitude",
            "c.speed",
            "c.alert_time",
            "c.vehicle_time",
            col("t.delay"),
            from_unixtime(col("t.event_ts").cast("long")).cast("timestamp").alias("trip_update_time"),
        )
    )


def aggregate_for_bronze(df: DataFrame) -> DataFrame:
    """Aggregate detail rows into per-(alert, route) impact summaries for Bronze."""
    return (
        df.groupBy("alert_id", "route_id")
        .agg(
            countDistinct("vehicle_id").alias("vehicles_affected"),
            countDistinct("trip_id").alias("trips_affected"),
            avg("delay").cast("float").alias("avg_delay_at_time"),
        )
        .withColumn("correlation_id", spark_uuid())
        .withColumn("detected_at", current_timestamp())
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
    STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
    MAX_OFFSETS_PER_TRIGGER = {
        "at.service_alerts": int(os.getenv("SERVICE_ALERTS_MAX_OFFSETS_PER_TRIGGER", "200")),
        "at.vehicle_positions": int(os.getenv("VEHICLE_POSITIONS_MAX_OFFSETS_PER_TRIGGER", "4000")),
        "at.trip_updates": int(os.getenv("TRIP_UPDATES_MAX_OFFSETS_PER_TRIGGER", "2000")),
    }
    CHECKPOINT_BASE = os.getenv("CHECKPOINT_PATH", "/tmp/checkpoints")
    OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/tmp/bronze")
    OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "parquet")
    SINK_TOPIC = "at.alerts"

    spark = (
        SparkSession.builder
        .appName("alert_correlation")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
                "org.apache.spark:spark-avro_2.13:4.1.1")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1536m"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # fetch avro schemas from SR
    topics = ["at.service_alerts", "at.vehicle_positions", "at.trip_updates"]
    schemas = {}
    for topic in topics:
        resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{topic}-value/versions/latest", timeout=10)
        resp.raise_for_status()
        schemas[topic] = resp.json()["schema"]

    def read_stream(topic):
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", STARTING_OFFSETS)
            .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER[topic])
            .load()
        )

    def parse_avro(raw, topic):
        return raw.select(
            from_avro(expr("substring(value, 6)"), schemas[topic]).alias("data"),
            col("timestamp").alias("kafka_ts"),
        )

    # --- three streams, watermarked on event_ts (business time) ---
    # PRD: "event_ts is the watermark column for all streaming queries"

    # Single read for service_alerts — fork into route-specific and network-wide paths.
    # Spark allows multiple writeStream queries from the same readStream DataFrame.
    sa_raw = parse_avro(read_stream("at.service_alerts"), "at.service_alerts")
    sa_flat = sa_raw.select(
        col("data.id").alias("alert_id"),
        col("data.route_id").alias("route_id"),
        col("data.cause").alias("cause"),
        col("data.effect").alias("effect"),
        col("data.header_text").alias("header_text"),
        col("data.description_text").alias("description_text"),
        from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_ts"),
    )

    # Alerts WITH route_id → join pipeline
    alerts = (
        sa_flat
        .filter(col("route_id").isNotNull())
        .withWatermark("event_ts", WATERMARK_ALERTS)
        # PRD edge case: deduplicate by (alert_id, route_id) to prevent
        # cartesian explosion from alert storms.
        .dropDuplicatesWithinWatermark(["alert_id", "route_id"])
    )

    # Alerts WITHOUT route_id → pass through directly (network-wide alerts)
    # PRD: "Network-wide alerts are passed through to at.alerts topic
    # directly without vehicle/trip correlation."
    network_alerts = (
        sa_flat
        .filter(col("route_id").isNull())
        .withWatermark("event_ts", WATERMARK_ALERTS)
    )

    vp_raw = parse_avro(read_stream("at.vehicle_positions"), "at.vehicle_positions")
    positions = (
        vp_raw
        .select(
            col("data.vehicle_id").alias("vehicle_id"),
            col("data.trip_id").alias("trip_id"),
            col("data.route_id").alias("route_id"),
            col("data.latitude").alias("latitude"),
            col("data.longitude").alias("longitude"),
            col("data.speed").alias("speed"),
            from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_ts"),
        )
        .filter((col("route_id").isNotNull()) & (col("route_id") != ""))
        .withWatermark("event_ts", WATERMARK_POSITIONS)
    )

    tu_raw = parse_avro(read_stream("at.trip_updates"), "at.trip_updates")
    trip_updates = (
        tu_raw
        .select(
            col("data.trip_id").alias("trip_id"),
            col("data.route_id").alias("route_id"),
            col("data.delay").alias("delay"),
            from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_ts"),
        )
        .filter((col("route_id").isNotNull()) & (col("route_id") != ""))
        .withWatermark("event_ts", WATERMARK_TRIP_UPDATES)
    )

    # --- chained 3-stream join ---
    correlated = correlate_alerts_with_positions(alerts, positions)
    enriched = enrich_with_trip_updates(correlated, trip_updates)

    # --- foreachBatch: write to both Kafka and Bronze ---

    def write_correlation_batch(batch_df, batch_id):
        """Write each micro-batch to Kafka (at.alerts) and Bronze table."""
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

            # 2. Write to Bronze table (bronze.alert_correlations)
            # PRD: aggregated per (alert_id, route_id) — not detail rows
            bronze_df = aggregate_for_bronze(batch_df).withColumn(
                "event_date", to_date(current_timestamp())
            )
            (
                bronze_df.write
                .format(OUTPUT_FORMAT)
                .mode("append")
                .partitionBy("event_date")
                .save(f"{OUTPUT_PATH}/alert_correlations")
            )
        finally:
            batch_df.unpersist()

    query = (
        enriched.writeStream
        .foreachBatch(write_correlation_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/alert_correlations")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .queryName("alert_correlations")
        .start()
    )

    # --- pass-through query for network-wide alerts (no route_id) ---
    network_query = (
        network_alerts
        .select(
            lit("network_alert").cast("string").alias("key"),
            to_json(struct(
                lit("network_alert").alias("alert_type"),
                col("alert_id"),
                col("cause"),
                col("effect"),
                col("header_text"),
                col("description_text"),
                col("event_ts"),
                current_timestamp().alias("detected_at"),
            )).alias("value"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", SINK_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/network_alerts_passthrough")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .queryName("network_alerts_passthrough")
        .start()
    )

    print(f"Alert correlation job started — 3-stream join, window={JOIN_WINDOW}")
    print(f"  Watermarks: alerts={WATERMARK_ALERTS}, positions={WATERMARK_POSITIONS}, "
          f"trip_updates={WATERMARK_TRIP_UPDATES}")

    from src.streaming._shutdown import run_until_shutdown
    run_until_shutdown(spark, query, network_query, job_label="alert_correlation")
