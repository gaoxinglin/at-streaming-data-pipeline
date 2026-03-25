from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, when, from_unixtime
import requests


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
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "at.vehicle_positions")
    .option("startingOffsets", "earliest")
    .load()
)

resp = requests.get("http://localhost:8081/subjects/at.vehicle_positions-value/versions/latest")
schema_str = resp.json()["schema"]

parsed = df.select(
    col("key").cast("string").alias("vehicle_id"),
    from_avro(expr("substring(value, 6)"), schema_str).alias("data"),
)

enriched = parsed.select(
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

query.awaitTermination(timeout=60)
print("done")
