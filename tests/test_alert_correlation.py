"""Unit tests for Q4 alert correlation — 3-stream join logic."""

import pytest
from datetime import datetime

from pyspark.sql.types import (
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.streaming.alert_correlation_job import (
    aggregate_for_bronze,
    correlate_alerts_with_positions,
    enrich_with_trip_updates,
)


# --- schemas ---


def _alert_schema():
    return StructType(
        [
            StructField("alert_id", StringType()),
            StructField("route_id", StringType()),
            StructField("cause", StringType()),
            StructField("effect", StringType()),
            StructField("header_text", StringType()),
            StructField("event_ts", TimestampType()),
        ]
    )


def _vp_schema():
    return StructType(
        [
            StructField("vehicle_id", StringType()),
            StructField("trip_id", StringType()),
            StructField("route_id", StringType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("speed", FloatType()),
            StructField("event_ts", TimestampType()),
        ]
    )


def _tu_schema():
    return StructType(
        [
            StructField("trip_id", StringType()),
            StructField("route_id", StringType()),
            StructField("delay", IntegerType()),
            StructField("event_ts", TimestampType()),
        ]
    )


# --- helpers ---

T0 = datetime(2026, 3, 27, 10, 0, 0)
# T_30S is within 60s window; T5 is 5 min away (outside 60s window)
T_30S = datetime(2026, 3, 27, 10, 0, 30)
T5 = datetime(2026, 3, 27, 10, 5, 0)


def _make_alert(spark, route_id="NX1-203", alert_id="A1", time=T0):
    return spark.createDataFrame(
        [
            {
                "alert_id": alert_id,
                "route_id": route_id,
                "cause": "CONSTRUCTION",
                "effect": "DETOUR",
                "header_text": "Road works",
                "event_ts": time,
            }
        ],
        schema=_alert_schema(),
    )


def _make_vp(spark, route_id="NX1-203", vehicle_id="V1", trip_id="T100", time=T_30S):
    return spark.createDataFrame(
        [
            {
                "vehicle_id": vehicle_id,
                "trip_id": trip_id,
                "route_id": route_id,
                "latitude": -36.84,
                "longitude": 174.76,
                "speed": 25.0,
                "event_ts": time,
            }
        ],
        schema=_vp_schema(),
    )


def _make_tu(spark, route_id="NX1-203", trip_id="T100", delay=120, time=T_30S):
    return spark.createDataFrame(
        [
            {
                "trip_id": trip_id,
                "route_id": route_id,
                "delay": delay,
                "event_ts": time,
            }
        ],
        schema=_tu_schema(),
    )


# --- correlation tests (alerts ⋈ positions) ---


def test_matching_route_joins(spark):
    """Alert and VP on same route within window should join."""
    result = correlate_alerts_with_positions(_make_alert(spark), _make_vp(spark))
    assert result.count() == 1
    row = result.first()
    assert row.alert_id == "A1"
    assert row.vehicle_id == "V1"
    assert row.route_id == "NX1-203"


def test_different_route_no_join(spark):
    """Alert and VP on different routes should not join."""
    result = correlate_alerts_with_positions(
        _make_alert(spark, route_id="NX1-203"), _make_vp(spark, route_id="OUT-202")
    )
    assert result.count() == 0


def test_outside_time_window_no_join(spark):
    """Events >60s apart should not join (JOIN_WINDOW = 60 seconds)."""
    result = correlate_alerts_with_positions(
        _make_alert(spark, time=datetime(2026, 3, 27, 10, 0, 0)),
        _make_vp(spark, time=datetime(2026, 3, 27, 10, 2, 0)),
    )
    assert result.count() == 0


def test_multiple_vehicles_on_route(spark):
    """Multiple vehicles on affected route should all be tagged."""
    positions = spark.createDataFrame(
        [
            {
                "vehicle_id": "V1",
                "trip_id": "T100",
                "route_id": "NX1-203",
                "latitude": -36.84,
                "longitude": 174.76,
                "speed": 20.0,
                "event_ts": T_30S,
            },
            {
                "vehicle_id": "V2",
                "trip_id": "T200",
                "route_id": "NX1-203",
                "latitude": -36.85,
                "longitude": 174.77,
                "speed": 30.0,
                "event_ts": T_30S,
            },
        ],
        schema=_vp_schema(),
    )
    result = correlate_alerts_with_positions(_make_alert(spark), positions)
    assert result.count() == 2
    vehicles = {r.vehicle_id for r in result.collect()}
    assert vehicles == {"V1", "V2"}


def test_correlation_output_columns(spark):
    result = correlate_alerts_with_positions(_make_alert(spark), _make_vp(spark))
    expected = {
        "alert_id",
        "route_id",
        "cause",
        "effect",
        "header_text",
        "vehicle_id",
        "trip_id",
        "latitude",
        "longitude",
        "speed",
        "alert_time",
        "vehicle_time",
        "event_ts",
    }
    assert set(result.columns) == expected


# --- enrichment tests (correlated ⋈ trip_updates) ---


def test_enrich_with_delay(spark):
    """Correlated events enriched with matching trip_update delay."""
    correlated = correlate_alerts_with_positions(_make_alert(spark), _make_vp(spark))
    enriched = enrich_with_trip_updates(correlated, _make_tu(spark, delay=360))
    assert enriched.count() == 1
    row = enriched.first()
    assert row.delay == 360
    assert row.trip_id == "T100"


def test_no_matching_trip_no_output(spark):
    """Inner join: no matching trip_update means no output row."""
    correlated = correlate_alerts_with_positions(_make_alert(spark), _make_vp(spark))
    # trip_id mismatch — vehicle is on T100 but trip_update is for T999
    enriched = enrich_with_trip_updates(correlated, _make_tu(spark, trip_id="T999"))
    assert enriched.count() == 0


def test_enriched_output_columns(spark):
    correlated = correlate_alerts_with_positions(_make_alert(spark), _make_vp(spark))
    enriched = enrich_with_trip_updates(correlated, _make_tu(spark))
    expected = {
        "alert_id",
        "route_id",
        "cause",
        "effect",
        "header_text",
        "vehicle_id",
        "trip_id",
        "latitude",
        "longitude",
        "speed",
        "alert_time",
        "vehicle_time",
        "delay",
        "trip_update_time",
    }
    assert set(enriched.columns) == expected


# --- bronze aggregation tests ---


def test_aggregate_for_bronze_columns(spark):
    """Bronze output should match PRD: aggregated per (alert_id, route_id)."""
    correlated = correlate_alerts_with_positions(_make_alert(spark), _make_vp(spark))
    enriched = enrich_with_trip_updates(correlated, _make_tu(spark))
    bronze = aggregate_for_bronze(enriched)
    expected = {
        "correlation_id",
        "alert_id",
        "route_id",
        "vehicles_affected",
        "trips_affected",
        "avg_delay_at_time",
        "detected_at",
    }
    assert set(bronze.columns) == expected


def test_aggregate_for_bronze_counts(spark):
    """Two vehicles on same alert/route → vehicles_affected=2, trips_affected=2."""
    positions = spark.createDataFrame(
        [
            {
                "vehicle_id": "V1",
                "trip_id": "T100",
                "route_id": "NX1-203",
                "latitude": -36.84,
                "longitude": 174.76,
                "speed": 20.0,
                "event_ts": T_30S,
            },
            {
                "vehicle_id": "V2",
                "trip_id": "T200",
                "route_id": "NX1-203",
                "latitude": -36.85,
                "longitude": 174.77,
                "speed": 30.0,
                "event_ts": T_30S,
            },
        ],
        schema=_vp_schema(),
    )
    trip_updates = spark.createDataFrame(
        [
            {"trip_id": "T100", "route_id": "NX1-203", "delay": 120, "event_ts": T_30S},
            {"trip_id": "T200", "route_id": "NX1-203", "delay": 240, "event_ts": T_30S},
        ],
        schema=_tu_schema(),
    )

    correlated = correlate_alerts_with_positions(_make_alert(spark), positions)
    enriched = enrich_with_trip_updates(correlated, trip_updates)
    bronze = aggregate_for_bronze(enriched)

    assert bronze.count() == 1
    row = bronze.first()
    assert row.vehicles_affected == 2
    assert row.trips_affected == 2
    assert row.avg_delay_at_time == pytest.approx(180.0)  # (120+240)/2
