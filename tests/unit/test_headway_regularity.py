"""Unit tests for Q3 headway regularity windowed aggregation."""

import pytest
from datetime import datetime
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType, TimestampType,
)

from src.streaming.detection.headway import compute_headway_regularity


def _vp_schema():
    return StructType([
        StructField("route_id", StringType()),
        StructField("direction_id", IntegerType()),
        StructField("start_time", StringType()),
        StructField("delay", IntegerType()),
        StructField("event_ts", TimestampType()),
    ])


def _make_vps(spark, rows):
    return spark.createDataFrame(rows, schema=_vp_schema())


def test_empty_route_filtered(spark):
    rows = [
        {"route_id": "", "direction_id": 0, "start_time": "08:00:00", "delay": 0,
         "event_ts": datetime(2026, 3, 27, 10, 0, 0)},
        {"route_id": "NX1-203", "direction_id": 0, "start_time": "08:10:00", "delay": 0,
         "event_ts": datetime(2026, 3, 27, 10, 0, 0)},
    ]
    routes = [r.route_id for r in compute_headway_regularity(_make_vps(spark, rows)).collect()]
    assert "NX1-203" in routes
    assert "" not in routes


def test_null_route_filtered(spark):
    rows = [{"route_id": None, "direction_id": 0, "start_time": "08:00:00", "delay": 0,
             "event_ts": datetime(2026, 3, 27, 10, 0, 0)}]
    assert compute_headway_regularity(_make_vps(spark, rows)).count() == 0


def test_headway_mean_and_trip_count(spark):
    t = datetime(2026, 3, 27, 10, 2, 0)
    rows = [
        {"route_id": "NX1", "direction_id": 0, "start_time": "08:00:00", "delay": 0, "event_ts": t},
        {"route_id": "NX1", "direction_id": 0, "start_time": "08:05:00", "delay": 0, "event_ts": t},
        {"route_id": "NX1", "direction_id": 0, "start_time": "08:10:00", "delay": 0, "event_ts": t},
    ]
    result = compute_headway_regularity(_make_vps(spark, rows)).collect()
    assert all(r.trip_count == 3 for r in result)
    assert all(r.headway_mean_s == pytest.approx(300.0, abs=0.1) for r in result)


def test_trip_count_below_threshold_has_null_cv(spark):
    t = datetime(2026, 3, 27, 10, 2, 0)
    rows = [
        {"route_id": "R1", "direction_id": 0, "start_time": "08:00:00", "delay": 0, "event_ts": t},
        {"route_id": "R1", "direction_id": 0, "start_time": "08:05:00", "delay": 0, "event_ts": t},
    ]
    result = compute_headway_regularity(_make_vps(spark, rows)).collect()
    assert all(r.headway_cv is None for r in result)
    assert all(r.is_bunching is False for r in result)


def test_separate_route_direction_groups(spark):
    t = datetime(2026, 3, 27, 10, 2, 0)
    rows = [
        {"route_id": "A", "direction_id": 0, "start_time": "08:00:00", "delay": 0, "event_ts": t},
        {"route_id": "A", "direction_id": 0, "start_time": "08:05:00", "delay": 0, "event_ts": t},
        {"route_id": "A", "direction_id": 0, "start_time": "08:10:00", "delay": 0, "event_ts": t},
        {"route_id": "A", "direction_id": 1, "start_time": "09:00:00", "delay": 0, "event_ts": t},
        {"route_id": "A", "direction_id": 1, "start_time": "09:06:00", "delay": 0, "event_ts": t},
        {"route_id": "A", "direction_id": 1, "start_time": "09:12:00", "delay": 0, "event_ts": t},
    ]
    groups = {(r.route_id, r.direction_id)
              for r in compute_headway_regularity(_make_vps(spark, rows)).collect()}
    assert ("A", 0) in groups
    assert ("A", 1) in groups


def test_uniform_headway_not_bunching(spark):
    t = datetime(2026, 3, 27, 10, 2, 0)
    rows = [
        {"route_id": "R1", "direction_id": 0, "start_time": "08:00:00", "delay": 0, "event_ts": t},
        {"route_id": "R1", "direction_id": 0, "start_time": "08:05:00", "delay": 0, "event_ts": t},
        {"route_id": "R1", "direction_id": 0, "start_time": "08:10:00", "delay": 0, "event_ts": t},
    ]
    result = compute_headway_regularity(_make_vps(spark, rows)).collect()
    assert all(r.headway_cv == pytest.approx(0.0, abs=0.0001) for r in result)
    assert all(r.is_bunching is False for r in result)


def test_irregular_headway_is_bunching(spark):
    t = datetime(2026, 3, 27, 10, 2, 0)
    rows = [
        {"route_id": "R1", "direction_id": 0, "start_time": "08:00:00", "delay": 0, "event_ts": t},
        {"route_id": "R1", "direction_id": 0, "start_time": "08:01:00", "delay": 0, "event_ts": t},
        {"route_id": "R1", "direction_id": 0, "start_time": "08:10:00", "delay": 0, "event_ts": t},
    ]
    result = compute_headway_regularity(_make_vps(spark, rows)).collect()
    assert all(r.headway_cv is not None and r.headway_cv > 0.5 for r in result)
    assert all(r.is_bunching is True for r in result)


def test_start_time_over_24h_supported(spark):
    t = datetime(2026, 3, 27, 1, 2, 0)
    rows = [
        {"route_id": "NIGHT", "direction_id": 0, "start_time": "24:10:00", "delay": 0, "event_ts": t},
        {"route_id": "NIGHT", "direction_id": 0, "start_time": "24:20:00", "delay": 0, "event_ts": t},
        {"route_id": "NIGHT", "direction_id": 0, "start_time": "24:30:00", "delay": 0, "event_ts": t},
    ]
    assert compute_headway_regularity(_make_vps(spark, rows)).count() > 0


def test_output_columns(spark):
    t = datetime(2026, 3, 27, 10, 2, 0)
    rows = [
        {"route_id": "R1", "direction_id": 0, "start_time": "08:00:00", "delay": 0, "event_ts": t},
        {"route_id": "R1", "direction_id": 0, "start_time": "08:05:00", "delay": 0, "event_ts": t},
        {"route_id": "R1", "direction_id": 0, "start_time": "08:10:00", "delay": 0, "event_ts": t},
    ]
    expected = {
        "window_start", "window_end", "route_id", "direction_id", "trip_count",
        "headway_mean_s", "headway_stddev_s", "headway_cv", "is_bunching",
    }
    assert set(compute_headway_regularity(_make_vps(spark, rows)).columns) == expected
