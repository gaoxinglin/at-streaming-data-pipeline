"""
Single entry point for the AT streaming pipeline.

Runs all four Structured Streaming queries on one shared cluster:
  - Bronze ingestion (vehicle_positions, trip_updates, service_alerts)
  - Q1: delay alerts
  - Q2: vehicle stall detection
  - Q3: headway regularity / bus bunching
"""

import os
import sys

# When run as a Databricks workspace script, sys.path contains the script's
# directory (src/streaming/), not the repo root. Add repo root so that
# `from src.streaming.*` imports resolve correctly.
try:
    _repo_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)
except NameError:
    # Databricks ipykernel runs scripts via exec(compile(...)) which doesn't set __file__.
    # The repo root is already the working directory and on sys.path.
    pass

try:
    from dotenv import load_dotenv
except ImportError:

    def load_dotenv():  # dotenv not installed on Databricks cluster
        pass


from pyspark.sql import SparkSession

from src.streaming.bronze_ingestion import start as start_bronze
from src.streaming.delay_alert_job import start as start_delay_alert
from src.streaming.vehicle_stall_job import start as start_vehicle_stall
from src.streaming.headway_regularity_job import start as start_headway_regularity
from src.streaming.alerts_consumer_job import start as start_alerts_consumer
from src.streaming._shutdown import run_until_shutdown

if __name__ == "__main__":
    load_dotenv()

    spark = (
        SparkSession.builder.appName("at_streaming_pipeline")
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"),
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    queries = [
        *start_bronze(spark),
        *start_delay_alert(spark),
        *start_vehicle_stall(spark),
        *start_headway_regularity(spark),
        *start_alerts_consumer(spark),
    ]

    print(f"All {len(queries)} streaming queries running on one cluster")
    run_until_shutdown(spark, *queries, job_label="at_streaming_pipeline")
