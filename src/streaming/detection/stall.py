"""Q2 vehicle stall detection — pure functions, no Spark session dependency."""

import math
import uuid
from typing import Iterator

import pandas as pd
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
    TimestampType,
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

STALL_THRESHOLD = 3      # consecutive readings within radius
STALL_RADIUS_M = 15.0    # metres (PRD: urban GPS CEP ≤ 15 m)
STALL_MIN_SPAN_S = 60    # minimum first-to-last span (PRD: prevents duplicate/jitter events)
STALL_MAX_SPAN_S = 600   # maximum span (PRD: one full off-peak polling cycle)
STATE_TIMEOUT_MS = 20 * 60 * 1000  # 20 minutes in ms

# schema for emitted stall events
STALL_EVENT_SCHEMA = StructType([
    StructField("stall_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("route_id", StringType()),
    StructField("trip_id", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("reading_count", IntegerType()),
    StructField("stall_duration_s", IntegerType()),
    StructField("first_seen", TimestampType()),
    StructField("stall_detected_ts", LongType()),
])

# schema for per-vehicle state stored between micro-batches
STATE_SCHEMA = StructType([
    StructField("anchor_lat", DoubleType()),
    StructField("anchor_lon", DoubleType()),
    StructField("count", IntegerType()),
    StructField("first_ts", LongType()),
    StructField("last_route_id", StringType()),
    StructField("last_trip_id", StringType()),
    StructField("already_emitted", IntegerType()),  # bool as int for pandas compat
])


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres between two GPS points."""
    R = 6_371_000
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def detect_stalls(
    key: tuple,
    readings: Iterator[pd.DataFrame],
    state: GroupState,
) -> Iterator[pd.DataFrame]:
    """
    Per-vehicle stateful stall detection.

    Emits a stall event when 3 consecutive readings fall within STALL_RADIUS_M
    AND the event_ts span from first to third is between STALL_MIN_SPAN_S and
    STALL_MAX_SPAN_S. State resets on route_id or trip_id change.
    """
    if state.hasTimedOut:
        state.remove()
        return iter([pd.DataFrame(columns=[f.name for f in STALL_EVENT_SCHEMA.fields])])

    if state.exists:
        s = state.get
        anchor_lat = float(s[0])
        anchor_lon = float(s[1])
        count = int(s[2])
        first_ts = int(s[3])
        last_route = str(s[4])
        last_trip = str(s[5])
        already_emitted = int(s[6])
    else:
        anchor_lat = anchor_lon = 0.0
        count = 0
        first_ts = 0
        last_route = ""
        last_trip = ""
        already_emitted = 0

    events = []

    for batch in readings:
        batch = batch.sort_values("timestamp")
        for _, row in batch.iterrows():
            lat, lon = float(row["latitude"]), float(row["longitude"])
            ts = int(row["timestamp"])
            route = str(row["route_id"]) if pd.notna(row["route_id"]) else ""
            trip = str(row["trip_id"]) if pd.notna(row["trip_id"]) else ""

            if count == 0:
                anchor_lat, anchor_lon = lat, lon
                count = 1
                first_ts = ts
                last_route = route
                last_trip = trip
            elif route != last_route or trip != last_trip:
                # route or trip changed — state reset prevents cross-trip false stalls
                anchor_lat, anchor_lon = lat, lon
                count = 1
                first_ts = ts
                last_route = route
                last_trip = trip
                already_emitted = 0
            elif haversine_m(anchor_lat, anchor_lon, lat, lon) <= STALL_RADIUS_M:
                count += 1
                last_route = route or last_route
                last_trip = trip or last_trip
            else:
                # moved beyond radius — reset anchor
                anchor_lat, anchor_lon = lat, lon
                count = 1
                first_ts = ts
                last_route = route
                last_trip = trip
                already_emitted = 0

            if count >= STALL_THRESHOLD and count > already_emitted:
                span = ts - first_ts
                if span < STALL_MIN_SPAN_S:
                    # readings too close together — wait for more to accumulate
                    pass
                elif span > STALL_MAX_SPAN_S:
                    # readings too spread out — reset and start fresh from here
                    anchor_lat, anchor_lon = lat, lon
                    count = 1
                    first_ts = ts
                    already_emitted = 0
                else:
                    events.append({
                        "stall_id": str(uuid.uuid4()),
                        "vehicle_id": key[0],
                        "route_id": last_route,
                        "trip_id": last_trip,
                        "latitude": anchor_lat,
                        "longitude": anchor_lon,
                        "reading_count": count,
                        "stall_duration_s": span,
                        "first_seen": pd.Timestamp(first_ts, unit="s"),
                        "stall_detected_ts": ts,
                    })
                    already_emitted = count

    state.update((anchor_lat, anchor_lon, count, first_ts, last_route, last_trip, already_emitted))
    state.setTimeoutDuration(STATE_TIMEOUT_MS)

    if events:
        return iter([pd.DataFrame(events)])
    else:
        return iter([pd.DataFrame(columns=[f.name for f in STALL_EVENT_SCHEMA.fields])])
