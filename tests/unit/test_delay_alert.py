"""Unit tests for Q1 delay alert detection logic."""

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.streaming.detection.delay import detect_delays


def _tu_schema():
    return StructType(
        [
            StructField("id", StringType()),
            StructField("trip_id", StringType()),
            StructField("route_id", StringType()),
            StructField("direction_id", IntegerType()),
            StructField("start_time", StringType()),
            StructField("start_date", StringType()),
            StructField("schedule_relationship", IntegerType()),
            StructField("delay", IntegerType()),
            StructField("timestamp", LongType()),
            StructField("is_deleted", BooleanType()),
        ]
    )


def _make_tu(spark, **overrides):
    defaults = {
        "id": "trip-001",
        "trip_id": "T100",
        "route_id": "R200",
        "direction_id": 0,
        "start_time": "08:00:00",
        "start_date": "20260327",
        "schedule_relationship": 0,
        "delay": 0,
        "timestamp": 1774345000,
        "is_deleted": False,
    }
    defaults.update(overrides)
    return spark.createDataFrame([defaults], schema=_tu_schema())


def test_below_threshold_filtered_out(spark):
    assert detect_delays(_make_tu(spark, delay=300)).count() == 0


def test_above_threshold_passes(spark):
    assert detect_delays(_make_tu(spark, delay=301)).count() == 1


def test_negative_delay_filtered_out(spark):
    assert detect_delays(_make_tu(spark, delay=-120)).count() == 0


def test_severity_moderate(spark):
    row = detect_delays(_make_tu(spark, delay=400)).first()
    assert row.severity == "MODERATE"


def test_severity_high(spark):
    row = detect_delays(_make_tu(spark, delay=900)).first()
    assert row.severity == "HIGH"


def test_severity_severe(spark):
    row = detect_delays(_make_tu(spark, delay=1500)).first()
    assert row.severity == "SEVERE"


def test_alert_has_expected_columns(spark):
    df = detect_delays(_make_tu(spark, delay=600))
    expected = {
        "event_id",
        "trip_id",
        "route_id",
        "delay",
        "severity",
        "start_time",
        "start_date",
        "event_timestamp",
        "detected_at",
    }
    assert set(df.columns) == expected


def test_fields_passthrough(spark):
    row = detect_delays(
        _make_tu(spark, trip_id="T999", route_id="R888", delay=500)
    ).first()
    assert row.trip_id == "T999"
    assert row.route_id == "R888"
    assert row.delay == 500
