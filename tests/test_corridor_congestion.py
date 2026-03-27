"""Unit tests for Q3 corridor congestion windowed aggregation."""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, FloatType, StringType, StructField, StructType, TimestampType,
)

from src.streaming.corridor_congestion_job import compute_corridor_congestion


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    yield s
    s.stop()


def _vp_schema():
    return StructType([
        StructField("route_id", StringType()),
        StructField("speed", FloatType()),
        StructField("event_time", TimestampType()),
    ])


def _make_vps(spark, rows):
    """Create a DataFrame of vehicle positions for testing.
    Each row is a dict with route_id, speed, event_time."""
    return spark.createDataFrame(rows, schema=_vp_schema())


# --- filtering ---

def test_empty_route_filtered(spark):
    """Records with empty route_id are excluded."""
    rows = [
        {"route_id": "", "speed": 30.0, "event_time": datetime(2026, 3, 27, 10, 0, 0)},
        {"route_id": "NX1-203", "speed": 30.0, "event_time": datetime(2026, 3, 27, 10, 0, 0)},
    ]
    result = compute_corridor_congestion(_make_vps(spark, rows))
    routes = [r.route_id for r in result.collect()]
    assert "NX1-203" in routes
    assert "" not in routes


def test_null_route_filtered(spark):
    """Records with null route_id are excluded."""
    rows = [
        {"route_id": None, "speed": 30.0, "event_time": datetime(2026, 3, 27, 10, 0, 0)},
    ]
    result = compute_corridor_congestion(_make_vps(spark, rows))
    assert result.count() == 0


# --- aggregation ---

def test_avg_speed_calculation(spark):
    """Average speed is computed correctly within a window."""
    t = datetime(2026, 3, 27, 10, 2, 0)  # all in same window
    rows = [
        {"route_id": "NX1", "speed": 20.0, "event_time": t},
        {"route_id": "NX1", "speed": 40.0, "event_time": t},
        {"route_id": "NX1", "speed": 60.0, "event_time": t},
    ]
    result = compute_corridor_congestion(_make_vps(spark, rows))
    # multiple windows will contain this point; all should have same avg
    avgs = [r.avg_speed_kmh for r in result.collect()]
    assert all(a == 40.0 for a in avgs)


def test_vehicle_count(spark):
    """Vehicle count reflects number of readings in window."""
    t = datetime(2026, 3, 27, 10, 2, 0)
    rows = [
        {"route_id": "R1", "speed": 10.0, "event_time": t},
        {"route_id": "R1", "speed": 20.0, "event_time": t},
    ]
    result = compute_corridor_congestion(_make_vps(spark, rows))
    counts = [r.vehicle_count for r in result.collect()]
    assert all(c == 2 for c in counts)


def test_separate_routes(spark):
    """Different routes are aggregated independently."""
    t = datetime(2026, 3, 27, 10, 2, 0)
    rows = [
        {"route_id": "A", "speed": 10.0, "event_time": t},
        {"route_id": "B", "speed": 50.0, "event_time": t},
    ]
    result = compute_corridor_congestion(_make_vps(spark, rows))
    route_speeds = {r.route_id: r.avg_speed_kmh for r in result.collect()}
    # each route appears in multiple sliding windows, but speed is consistent
    assert 10.0 in route_speeds.values()
    assert 50.0 in route_speeds.values()


# --- congestion classification ---

def test_congested_classification(spark):
    """avg_speed < 10 → CONGESTED."""
    t = datetime(2026, 3, 27, 10, 2, 0)
    rows = [{"route_id": "R1", "speed": 5.0, "event_time": t}]
    result = compute_corridor_congestion(_make_vps(spark, rows))
    levels = {r.congestion_level for r in result.collect()}
    assert levels == {"CONGESTED"}


def test_slow_classification(spark):
    """10 <= avg_speed < 25 → SLOW."""
    t = datetime(2026, 3, 27, 10, 2, 0)
    rows = [{"route_id": "R1", "speed": 18.0, "event_time": t}]
    result = compute_corridor_congestion(_make_vps(spark, rows))
    levels = {r.congestion_level for r in result.collect()}
    assert levels == {"SLOW"}


def test_free_flow_classification(spark):
    """avg_speed >= 25 → FREE_FLOW."""
    t = datetime(2026, 3, 27, 10, 2, 0)
    rows = [{"route_id": "R1", "speed": 45.0, "event_time": t}]
    result = compute_corridor_congestion(_make_vps(spark, rows))
    levels = {r.congestion_level for r in result.collect()}
    assert levels == {"FREE_FLOW"}


# --- output schema ---

def test_output_columns(spark):
    t = datetime(2026, 3, 27, 10, 2, 0)
    rows = [{"route_id": "R1", "speed": 30.0, "event_time": t}]
    result = compute_corridor_congestion(_make_vps(spark, rows))
    expected = {"window_start", "window_end", "route_id", "avg_speed_kmh",
                "vehicle_count", "congestion_level"}
    assert set(result.columns) == expected
