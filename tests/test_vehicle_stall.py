"""Unit tests for Q2 vehicle stall detection."""

import pytest
import pandas as pd
from unittest.mock import MagicMock

from src.streaming.vehicle_stall_job import (
    haversine_m, detect_stalls, STALL_EVENT_SCHEMA, STALL_THRESHOLD,
)


# --- haversine tests ---

def test_haversine_same_point():
    """Distance from a point to itself is 0."""
    assert haversine_m(-36.84, 174.76, -36.84, 174.76) == 0.0


def test_haversine_known_distance():
    """Auckland CBD to ~111m north (0.001 degree latitude ≈ 111m)."""
    d = haversine_m(-36.840, 174.760, -36.839, 174.760)
    assert 100 < d < 120  # ~111m


def test_haversine_within_10m():
    """Two points ~5m apart should be < 10m."""
    # 0.00005 degree lat ≈ 5.5m
    d = haversine_m(-36.84000, 174.76000, -36.83995, 174.76000)
    assert d < 10


# --- stall detection logic tests ---

def _make_state(exists=False, data=None, timed_out=False):
    """Create a mock GroupState for testing."""
    state = MagicMock()
    state.hasTimedOut = timed_out
    state.exists = exists
    if data:
        state.get = data
    return state


def _readings_iter(rows):
    """Wrap rows into an iterator of DataFrames (as applyInPandasWithState expects)."""
    df = pd.DataFrame(rows)
    return iter([df])


def test_no_stall_below_threshold():
    """2 readings at same spot shouldn't trigger (threshold=3)."""
    readings = _readings_iter([
        {"vehicle_id": "V1", "route_id": "R1", "latitude": -36.84, "longitude": 174.76, "timestamp": 1000},
        {"vehicle_id": "V1", "route_id": "R1", "latitude": -36.84, "longitude": 174.76, "timestamp": 1030},
    ])
    state = _make_state()
    results = list(detect_stalls(("V1",), readings, state))
    events = pd.concat(results, ignore_index=True)
    assert len(events) == 0


def test_stall_at_threshold():
    """3 readings at same spot should trigger a stall event."""
    readings = _readings_iter([
        {"vehicle_id": "V1", "route_id": "R1", "latitude": -36.84, "longitude": 174.76, "timestamp": 1000},
        {"vehicle_id": "V1", "route_id": "R1", "latitude": -36.84, "longitude": 174.76, "timestamp": 1030},
        {"vehicle_id": "V1", "route_id": "R1", "latitude": -36.84, "longitude": 174.76, "timestamp": 1060},
    ])
    state = _make_state()
    results = list(detect_stalls(("V1",), readings, state))
    events = pd.concat(results, ignore_index=True)
    assert len(events) == 1
    assert events.iloc[0]["vehicle_id"] == "V1"
    assert events.iloc[0]["consecutive_count"] == 3
    assert events.iloc[0]["stall_start_ts"] == 1000


def test_movement_resets_count():
    """Moving >10m resets the stall counter."""
    readings = _readings_iter([
        {"vehicle_id": "V1", "route_id": "R1", "latitude": -36.84, "longitude": 174.76, "timestamp": 1000},
        {"vehicle_id": "V1", "route_id": "R1", "latitude": -36.84, "longitude": 174.76, "timestamp": 1030},
        # move ~111m north — resets
        {"vehicle_id": "V1", "route_id": "R1", "latitude": -36.839, "longitude": 174.76, "timestamp": 1060},
        {"vehicle_id": "V1", "route_id": "R1", "latitude": -36.839, "longitude": 174.76, "timestamp": 1090},
    ])
    state = _make_state()
    results = list(detect_stalls(("V1",), readings, state))
    events = pd.concat(results, ignore_index=True)
    assert len(events) == 0  # never hit 3 consecutive at either spot


def test_stall_with_prior_state():
    """Prior state of 2 readings + 1 new reading = stall at 3."""
    # state: anchor at (-36.84, 174.76), count=2, first_ts=900
    prior = (-36.84, 174.76, 2, 900, "R1", 0)
    readings = _readings_iter([
        {"vehicle_id": "V1", "route_id": "R1", "latitude": -36.84, "longitude": 174.76, "timestamp": 1000},
    ])
    state = _make_state(exists=True, data=prior)
    results = list(detect_stalls(("V1",), readings, state))
    events = pd.concat(results, ignore_index=True)
    assert len(events) == 1
    assert events.iloc[0]["consecutive_count"] == 3
    assert events.iloc[0]["stall_start_ts"] == 900  # from state


def test_timeout_clears_state():
    """Timed-out state should be removed, no events emitted."""
    state = _make_state(timed_out=True)
    results = list(detect_stalls(("V1",), iter([]), state))
    events = pd.concat(results, ignore_index=True)
    assert len(events) == 0
    state.remove.assert_called_once()


def test_stall_event_fields():
    """Stall event has all expected fields."""
    readings = _readings_iter([
        {"vehicle_id": "V1", "route_id": "NX1-203", "latitude": -36.84, "longitude": 174.76, "timestamp": 1000},
        {"vehicle_id": "V1", "route_id": "NX1-203", "latitude": -36.84, "longitude": 174.76, "timestamp": 1030},
        {"vehicle_id": "V1", "route_id": "NX1-203", "latitude": -36.84, "longitude": 174.76, "timestamp": 1060},
    ])
    state = _make_state()
    results = list(detect_stalls(("V1",), readings, state))
    events = pd.concat(results, ignore_index=True)
    expected_cols = {f.name for f in STALL_EVENT_SCHEMA.fields}
    assert set(events.columns) == expected_cols
    assert events.iloc[0]["route_id"] == "NX1-203"


def test_ongoing_stall_emits_updates():
    """4th and 5th readings at same spot emit additional events."""
    readings = _readings_iter([
        {"vehicle_id": "V1", "route_id": "R1", "latitude": -36.84, "longitude": 174.76, "timestamp": t}
        for t in [1000, 1030, 1060, 1090, 1120]
    ])
    state = _make_state()
    results = list(detect_stalls(("V1",), readings, state))
    events = pd.concat(results, ignore_index=True)
    # should get events at count 3, 4, 5
    assert len(events) == 3
    counts = events["consecutive_count"].tolist()
    assert counts == [3, 4, 5]
