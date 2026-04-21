"""Q1 delay alert detection — pure functions, no Spark session dependency."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, when
from pyspark.sql.functions import uuid as spark_uuid

DELAY_THRESHOLD = 300  # 5 minutes in seconds


def detect_delays(df: DataFrame) -> DataFrame:
    """Filter trip_updates with delay > 5 min and classify severity."""
    return (
        df.filter(col("delay") > DELAY_THRESHOLD)
        .select(
            spark_uuid().alias("event_id"),
            "trip_id",
            "route_id",
            "delay",
            when(col("delay") <= 600, lit("MODERATE"))   # 5-10 min
            .when(col("delay") <= 1200, lit("HIGH"))      # 10-20 min
            .otherwise(lit("SEVERE"))                     # 20+ min
            .alias("severity"),
            "start_time",
            "start_date",
            col("timestamp").alias("event_timestamp"),
            current_timestamp().alias("detected_at"),
        )
    )
