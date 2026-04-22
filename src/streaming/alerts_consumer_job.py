"""
alerts_consumer_job: at.alerts → bronze.alerts (Delta).

Normalises the three message shapes written by the detection jobs into the
unified bronze.alerts schema defined in the PRD. Deduplicates on alert_id
using Delta MERGE so at-least-once Kafka delivery never creates duplicate rows.

Message shapes on at.alerts (all plain JSON in cloud mode):
  Q1 delay:    {event_id, trip_id, route_id, delay, severity, event_timestamp, detected_at, ...}
  Q2 stall:    {stall_id, vehicle_id, route_id, trip_id, stall_detected_ts, ...}
  Q3 bunching: {alert_type:"bunching_alert", route_id, direction_id, window_start, detected_at, ...}
"""

import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit, to_date, when
from src.streaming import kafka_utils

load_dotenv()


def _normalize(df: DataFrame) -> DataFrame:
    """Parse raw JSON from at.alerts and produce the bronze.alerts row shape."""
    raw = df.select(col("value").cast("string").alias("j"))

    x = raw.select(
        "j",
        expr("get_json_object(j, '$.event_id')").alias("_q1_id"),
        expr("get_json_object(j, '$.stall_id')").alias("_q2_id"),
        expr("get_json_object(j, '$.route_id')").alias("route_id"),
        expr("get_json_object(j, '$.vehicle_id')").alias("vehicle_id"),
        expr("get_json_object(j, '$.trip_id')").alias("trip_id"),
        expr("get_json_object(j, '$.detected_at')").alias("_detected_at"),
        # event_ts source differs per job: epoch-seconds for Q1/Q2, ISO string for Q3
        expr("get_json_object(j, '$.event_timestamp')").alias("_ts_q1"),
        expr("get_json_object(j, '$.stall_detected_ts')").alias("_ts_q2"),
        expr("get_json_object(j, '$.window_start')").alias("_ts_q3"),
    )

    return x.select(
        when(col("_q1_id").isNotNull(), col("_q1_id"))
        .when(col("_q2_id").isNotNull(), col("_q2_id"))
        .otherwise(expr("uuid()"))
        .alias("alert_id"),
        when(col("_q1_id").isNotNull(), lit("DELAY"))
        .when(col("_q2_id").isNotNull(), lit("STALL"))
        .otherwise(lit("BUNCHING"))
        .alias("alert_type"),
        "route_id",
        "vehicle_id",
        "trip_id",
        when(col("_q1_id").isNotNull(), col("_ts_q1").cast("long").cast("timestamp"))
        .when(col("_q2_id").isNotNull(), col("_ts_q2").cast("long").cast("timestamp"))
        .otherwise(col("_ts_q3").cast("timestamp"))
        .alias("event_ts"),
        col("_detected_at").cast("timestamp").alias("detected_at"),
        col("j").alias("payload"),
        current_timestamp().alias("ingested_at"),
    )


def start(spark: SparkSession) -> list:
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
    MAX_OFFSETS = int(os.getenv("BRONZE_MAX_OFFSETS_PER_TRIGGER", "5000"))
    CHECKPOINT_BASE = os.getenv("CHECKPOINT_PATH", "/tmp/checkpoints")
    OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/tmp/bronze")
    OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "delta")
    SOURCE_TOPIC = "at.alerts"
    alerts_path = f"{OUTPUT_PATH}/alerts"

    raw = (
        spark.readStream.format("kafka")
        .options(**kafka_utils.kafka_options(KAFKA_BOOTSTRAP))
        .option("subscribe", kafka_utils.topic_name(SOURCE_TOPIC))
        .option("startingOffsets", STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .load()
    )

    normalised = _normalize(raw)

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        batch_df = batch_df.withColumn("event_date", to_date(col("event_ts")))
        batch_df.persist()
        try:
            if OUTPUT_FORMAT == "delta":
                from delta.tables import DeltaTable

                if DeltaTable.isDeltaTable(spark, alerts_path):
                    # MERGE deduplicates on alert_id — safe for at-least-once delivery
                    DeltaTable.forPath(spark, alerts_path).alias("t").merge(
                        batch_df.alias("s"),
                        "t.alert_id = s.alert_id",
                    ).whenNotMatchedInsertAll().execute()
                else:
                    batch_df.write.format("delta").partitionBy("event_date").save(
                        alerts_path
                    )
            else:
                batch_df.write.format(OUTPUT_FORMAT).mode("append").partitionBy(
                    "event_date"
                ).save(alerts_path)
        finally:
            batch_df.unpersist()

    query = (
        normalised.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/alerts_consumer")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .queryName("alerts_consumer")
        .start()
    )

    print(f"Alerts consumer started — {SOURCE_TOPIC} → {alerts_path}")
    return [query]


if __name__ == "__main__":
    load_dotenv()
    spark = (
        SparkSession.builder.appName("alerts_consumer")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "io.delta:delta-spark_2.12:3.0.0",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    from src.streaming._shutdown import run_until_shutdown

    run_until_shutdown(spark, *start(spark), job_label="alerts_consumer")
