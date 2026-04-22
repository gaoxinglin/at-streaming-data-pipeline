"""Unit tests for Q2 vehicle stall detection."""

import pandas as pd
from unittest.mock import MagicMock

from src.streaming.detection.stall import (
    haversine_m,
    detect_stalls,
    STALL_EVENT_SCHEMA,
    STALL_RADIUS_M,
)


def _make_state(exists=False, data=None, timed_out=False):
    state = MagicMock()
    state.hasTimedOut = timed_out
    state.exists = exists
    if data:
        state.get = data
    return state


def _readings_iter(rows):
    """Wrap rows into an iterator of DataFrames (as applyInPandasWithState expects)."""
    return iter([pd.DataFrame(rows)])


def test_haversine_same_point():
    assert haversine_m(-36.84, 174.76, -36.84, 174.76) == 0.0


def test_haversine_known_distance():
    """Auckland CBD to ~111m north (0.001 degree latitude ≈ 111m)."""
    d = haversine_m(-36.840, 174.760, -36.839, 174.760)
    assert 100 < d < 120


def test_haversine_within_radius():
    """Two points ~5m apart should be well within STALL_RADIUS_M (15m)."""
    d = haversine_m(-36.84000, 174.76000, -36.83995, 174.76000)
    assert d < STALL_RADIUS_M


def test_no_stall_below_threshold():
    """2 readings at same spot shouldn't trigger (threshold=3)."""
    readings = _readings_iter(
        [
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1000,
            },
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1030,
            },
        ]
    )
    events = pd.concat(
        list(detect_stalls(("V1",), readings, _make_state())), ignore_index=True
    )
    assert len(events) == 0


def test_stall_at_threshold():
    """3 readings at same spot spanning exactly 60s → stall event emitted."""
    readings = _readings_iter(
        [
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1000,
            },
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1030,
            },
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1060,
            },
        ]
    )
    events = pd.concat(
        list(detect_stalls(("V1",), readings, _make_state())), ignore_index=True
    )
    assert len(events) == 1
    assert events.iloc[0]["vehicle_id"] == "V1"
    assert events.iloc[0]["reading_count"] == 3
    assert events.iloc[0]["stall_duration_s"] == 60
    assert events.iloc[0]["first_seen"] == pd.Timestamp(1000, unit="s")
    assert events.iloc[0]["stall_id"] is not None


def test_stall_time_consistency_too_short():
    """3 readings spanning < 60s → no stall (span below minimum)."""
    readings = _readings_iter(
        [
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1000,
            },
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1010,
            },
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1020,
            },
        ]
    )
    events = pd.concat(
        list(detect_stalls(("V1",), readings, _make_state())), ignore_index=True
    )
    assert len(events) == 0


def test_movement_resets_count():
    """Moving >15m resets the stall counter."""
    readings = _readings_iter(
        [
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1000,
            },
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1030,
            },
            # move ~111m north — resets
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.839,
                "longitude": 174.76,
                "timestamp": 1060,
            },
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.839,
                "longitude": 174.76,
                "timestamp": 1090,
            },
        ]
    )
    events = pd.concat(
        list(detect_stalls(("V1",), readings, _make_state())), ignore_index=True
    )
    assert len(events) == 0


def test_trip_id_change_resets_state():
    """trip_id change mid-window resets state — no cross-trip false stall."""
    readings = _readings_iter(
        [
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1000,
            },
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1030,
            },
            # trip changes at terminus — state resets
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T2",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1060,
            },
        ]
    )
    events = pd.concat(
        list(detect_stalls(("V1",), readings, _make_state())), ignore_index=True
    )
    assert len(events) == 0


def test_stall_with_prior_state():
    """Prior state of 2 readings + 1 new reading = stall at 3."""
    prior = (-36.84, 174.76, 2, 900, "R1", "T1", 0, 0)  # 8-tuple; all_stopped_at=0
    readings = _readings_iter(
        [
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1000,
            },
        ]
    )
    events = pd.concat(
        list(detect_stalls(("V1",), readings, _make_state(exists=True, data=prior))),
        ignore_index=True,
    )
    assert len(events) == 1
    assert events.iloc[0]["reading_count"] == 3
    assert events.iloc[0]["first_seen"] == pd.Timestamp(900, unit="s")


def test_timeout_clears_state():
    state = _make_state(timed_out=True)
    events = pd.concat(list(detect_stalls(("V1",), iter([]), state)), ignore_index=True)
    assert len(events) == 0
    state.remove.assert_called_once()


def test_stall_event_fields():
    """Stall event has all STALL_EVENT_SCHEMA fields."""
    readings = _readings_iter(
        [
            {
                "vehicle_id": "V1",
                "route_id": "NX1-203",
                "trip_id": "T9",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1000,
            },
            {
                "vehicle_id": "V1",
                "route_id": "NX1-203",
                "trip_id": "T9",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1030,
            },
            {
                "vehicle_id": "V1",
                "route_id": "NX1-203",
                "trip_id": "T9",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": 1060,
            },
        ]
    )
    events = pd.concat(
        list(detect_stalls(("V1",), readings, _make_state())), ignore_index=True
    )
    assert set(events.columns) == {f.name for f in STALL_EVENT_SCHEMA.fields}
    assert events.iloc[0]["route_id"] == "NX1-203"
    assert events.iloc[0]["trip_id"] == "T9"


def test_ongoing_stall_emits_updates():
    """4th and 5th readings at same spot emit additional events."""
    readings = _readings_iter(
        [
            {
                "vehicle_id": "V1",
                "route_id": "R1",
                "trip_id": "T1",
                "latitude": -36.84,
                "longitude": 174.76,
                "timestamp": t,
            }
            for t in [1000, 1030, 1060, 1090, 1120]
        ]
    )
    events = pd.concat(
        list(detect_stalls(("V1",), readings, _make_state())), ignore_index=True
    )
    assert len(events) == 3
    assert events["reading_count"].tolist() == [3, 4, 5]
