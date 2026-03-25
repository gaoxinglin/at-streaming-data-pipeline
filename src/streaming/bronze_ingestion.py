import os
import signal

import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, when, from_unixtime

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = "at.vehicle_positions"

spark = (
    SparkSession.builder
    .appName("consumer")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
            "org.apache.spark:spark-avro_2.13:4.1.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10000)
    .load()
)

resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{TOPIC}-value/versions/latest")
schema_str = resp.json()["schema"]

parsed = df.select(
    col("key").cast("string").alias("vehicle_id"),
    from_avro(expr("substring(value, 6)"), schema_str).alias("data"),
    col("timestamp").alias("kafka_timestamp"),
)

# drop late events older than 10 minutes
watermarked = parsed.withWatermark("kafka_timestamp", "10 minutes")

enriched = watermarked.select(
    "vehicle_id",
    "data.latitude",
    "data.longitude",
    "data.speed",
    "data.timestamp",
    "data.is_deleted",
    from_unixtime("data.timestamp").cast("timestamp").alias("event_time"),
    from_unixtime("data.timestamp").cast("date").alias("event_date"),
    when(
        (col("data.latitude").between(-37.1, -36.6)) &
        (col("data.longitude").between(174.4, 175.1)),
        True
    ).otherwise(False).alias("is_valid_gps"),
)

query = (
    enriched.writeStream
    .format("parquet")
    .option("path", "/tmp/bronze/vehicle_positions")
    .option("checkpointLocation", "/tmp/checkpoints/vehicle_positions")
    .partitionBy("event_date")
    .outputMode("append")
    .start()
)

# Ctrl+C or kill → set flag, main thread does the actual stop
_shutdown = False

def _stop(sig, frame):
    global _shutdown
    print(f"\nCaught signal {sig}, shutting down...")
    _shutdown = True

signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)

# poll so we can check the flag between iterations
while query.isActive:
    if _shutdown:
        query.stop()
        break
    query.awaitTermination(timeout=1)

print("done")
