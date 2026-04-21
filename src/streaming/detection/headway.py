"""Q3 headway regularity detection — pure functions, no Spark session dependency."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array_sort, col, collect_list, expr, lit, size, when, window,
)

WINDOW_DURATION = "10 minutes"
SLIDE_INTERVAL = "2 minutes"
WATERMARK_DELAY = "10 minutes"
BUNCHING_CV_THRESHOLD = 0.5


def _start_time_to_seconds_expr() -> str:
    """Convert HH:MM:SS (supports 24+ hours) into seconds since midnight."""
    return (
        "cast(split(start_time, ':')[0] as int) * 3600 + "
        "cast(split(start_time, ':')[1] as int) * 60 + "
        "cast(split(start_time, ':')[2] as int)"
    )


def compute_headway_regularity(df: DataFrame) -> DataFrame:
    """
    Compute route headway CV in sliding windows.

    Input df must have: route_id, direction_id, start_time, delay, event_ts.
    Approximates actual departure seconds as start_time + delay.

    NOTE: This uses trip_updates departure-headway approximation (v4 approach).
    The PRD v5.1 calls for VP-based stop-arrival headway, which requires
    stop_times.txt ingestion. Migration is tracked as Phase 3 pending work.
    """
    enriched = (
        df.filter((col("route_id").isNotNull()) & (col("route_id") != ""))
        .filter(col("start_time").isNotNull())
        .withColumn("scheduled_departure_s", expr(_start_time_to_seconds_expr()))
        .withColumn("delay_s", expr("coalesce(delay, 0)"))
        .withColumn("actual_departure_s", col("scheduled_departure_s") + col("delay_s"))
    )

    grouped = (
        enriched
        .groupBy(
            window(col("event_ts"), WINDOW_DURATION, SLIDE_INTERVAL),
            "route_id",
            "direction_id",
        )
        .agg(
            array_sort(collect_list(col("actual_departure_s"))).alias("sorted_departures"),
        )
        .withColumn("trip_count", size(col("sorted_departures")))
    )

    with_headways = (
        grouped
        .withColumn(
            "headways",
            expr(
                "case when size(sorted_departures) >= 2 then "
                "transform(sequence(2, size(sorted_departures)), "
                "i -> element_at(sorted_departures, i) - element_at(sorted_departures, i - 1)) "
                "else array() end"
            ),
        )
        .withColumn("headway_count", size(col("headways")))
        .withColumn(
            "headway_mean_s",
            when(
                col("headway_count") > 0,
                expr("aggregate(headways, 0D, (acc, x) -> acc + x) / headway_count"),
            ),
        )
        .withColumn(
            "headway_stddev_s",
            when(
                col("headway_count") > 0,
                expr(
                    "sqrt(aggregate(headways, 0D, "
                    "(acc, x) -> acc + pow(x - headway_mean_s, 2)) / headway_count)"
                ),
            ),
        )
        .withColumn(
            "headway_cv_raw",
            when(col("headway_mean_s") > 0, col("headway_stddev_s") / col("headway_mean_s")),
        )
    )

    return (
        with_headways
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "route_id",
            "direction_id",
            "trip_count",
            "headway_mean_s",
            "headway_stddev_s",
            when(col("trip_count") >= 3, col("headway_cv_raw")).otherwise(lit(None)).alias("headway_cv"),
            when((col("trip_count") >= 3) & (col("headway_cv_raw") > BUNCHING_CV_THRESHOLD), lit(True))
            .otherwise(lit(False))
            .alias("is_bunching"),
        )
    )
