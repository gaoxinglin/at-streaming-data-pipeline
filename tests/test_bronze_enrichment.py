"""Test enrichment logic using PySpark batch mode — no Kafka needed."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    FloatType, LongType, BooleanType, IntegerType,
)

from src.streaming.bronze_ingestion import (
    enrich_vehicle_positions,
    enrich_trip_updates,
    enrich_service_alerts,
)


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
    s.sparkContext.setLogLevel("OFF")
    yield s
    s.stop()


# -- vehicle positions --

def _vp_schema():
    return StructType([
        StructField("vehicle_id", StringType()),
        StructField("trip_id", StringType()),
        StructField("route_id", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("bearing", FloatType()),
        StructField("speed", FloatType()),
        StructField("current_stop_sequence", IntegerType()),
        StructField("stop_id", StringType()),
        StructField("current_status", StringType()),
        StructField("congestion_level", StringType()),
        StructField("occupancy_status", StringType()),
        StructField("timestamp", LongType()),
        StructField("ts_raw", LongType()),
        StructField("_raw_payload", StringType()),
    ])


def _make_vp(spark, **overrides):
    defaults = {
        "vehicle_id": "v1", "trip_id": "t1", "route_id": "r1",
        "latitude": -36.85, "longitude": 174.76,
        "bearing": None, "speed": 59.0,
        "current_stop_sequence": None, "stop_id": None,
        "current_status": None, "congestion_level": None,
        "occupancy_status": None,
        "timestamp": 1774345000, "ts_raw": 1774345000,
        "_raw_payload": "{}",
    }
    defaults.update(overrides)
    return spark.createDataFrame([tuple(defaults.values())], _vp_schema())


def test_vp_has_audit_fields(spark):
    result = enrich_vehicle_positions(_make_vp(spark)).collect()[0]
    assert result["event_id"] is not None
    assert result["ingestion_timestamp"] is not None


def test_vp_speed_passthrough(spark):
    """AT legacy API returns km/h directly — no conversion."""
    result = enrich_vehicle_positions(_make_vp(spark, speed=59.0)).collect()[0]
    assert result["speed"] == pytest.approx(59.0, abs=0.1)


def test_vp_event_date(spark):
    result = enrich_vehicle_positions(_make_vp(spark)).collect()[0]
    assert str(result["event_date"]) == "2026-03-24"


def test_vp_null_speed(spark):
    result = enrich_vehicle_positions(_make_vp(spark, speed=None)).collect()[0]
    assert result["speed"] is None


# -- trip updates --

def _tu_schema():
    return StructType([
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
        StructField("ts_raw", LongType()),
        StructField("_raw_payload", StringType()),
    ])


def _make_tu(spark, **overrides):
    defaults = {
        "id": "tu1", "trip_id": "t1", "route_id": "r1",
        "direction_id": 0, "start_time": "08:00:00",
        "start_date": "20260326", "schedule_relationship": 0,
        "delay": 360, "timestamp": 1774345000,
        "is_deleted": False, "ts_raw": 1774345000,
        "_raw_payload": "{}",
    }
    defaults.update(overrides)
    return spark.createDataFrame([tuple(defaults.values())], _tu_schema())


def test_tu_delay_preserved(spark):
    result = enrich_trip_updates(_make_tu(spark, delay=360)).collect()[0]
    assert result["delay"] == 360


def test_tu_event_date(spark):
    result = enrich_trip_updates(_make_tu(spark)).collect()[0]
    assert str(result["event_date"]) == "2026-03-24"


# -- service alerts --

def _sa_schema():
    return StructType([
        StructField("id", StringType()),
        StructField("route_id", StringType()),
        StructField("cause", StringType()),
        StructField("effect", StringType()),
        StructField("header_text", StringType()),
        StructField("description_text", StringType()),
        StructField("active_period_start", LongType()),
        StructField("active_period_end", LongType()),
        StructField("timestamp", LongType()),
        StructField("ts_raw", LongType()),
        StructField("_raw_payload", StringType()),
    ])


def _make_sa(spark, **overrides):
    defaults = {
        "id": "sa1", "route_id": "NX1",
        "cause": "CONSTRUCTION", "effect": "DETOUR",
        "header_text": "Detour on NX1", "description_text": "Road works",
        "active_period_start": 1774340000, "active_period_end": 1774350000,
        "timestamp": 1774345000, "ts_raw": 1774345000,
        "_raw_payload": "{}",
    }
    defaults.update(overrides)
    return spark.createDataFrame([tuple(defaults.values())], _sa_schema())


def test_sa_alert_id_renamed(spark):
    result = enrich_service_alerts(_make_sa(spark)).collect()[0]
    assert result["alert_id"] == "sa1"


def test_sa_has_audit_fields(spark):
    result = enrich_service_alerts(_make_sa(spark)).collect()[0]
    assert result["event_id"] is not None
    assert result["ingestion_timestamp"] is not None
