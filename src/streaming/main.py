"""
Single entry point for the AT streaming pipeline.

Runs all four Structured Streaming queries on one shared cluster:
  - Bronze ingestion (vehicle_positions, trip_updates, service_alerts)
  - Q1: delay alerts
  - Q2: vehicle stall detection
  - Q3: headway regularity / bus bunching
"""

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

from src.streaming.bronze_ingestion import start as start_bronze
from src.streaming.delay_alert_job import start as start_delay_alert
from src.streaming.vehicle_stall_job import start as start_vehicle_stall
from src.streaming.headway_regularity_job import start as start_headway_regularity
from src.streaming._shutdown import run_until_shutdown

if __name__ == "__main__":
    load_dotenv()

    spark = (
        SparkSession.builder
        .appName("at_streaming_pipeline")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "org.apache.spark:spark-avro_2.12:3.4.1")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    queries = [
        *start_bronze(spark),
        *start_delay_alert(spark),
        *start_vehicle_stall(spark),
        *start_headway_regularity(spark),
    ]

    print(f"All {len(queries)} streaming queries running on one cluster")
    run_until_shutdown(spark, *queries, job_label="at_streaming_pipeline")
