"""
Real-time dashboard for AT Streaming Pipeline Q1-Q3.

Reads directly from Kafka / Azure Event Hubs topics:
  - alerts           → Q1 delay alerts, Q2 stall events
  - headway_metrics  → Q3 headway regularity / bus bunching

Run with:
    streamlit run src/dashboard/app.py
"""

import json
import os
import time
from datetime import datetime

import pydeck as pdk

import pandas as pd
import streamlit as st
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENTHUBS_CONNECTION_STRING = os.getenv("EVENTHUBS_CONNECTION_STRING", "")
CLOUD_MODE = bool(EVENTHUBS_CONNECTION_STRING)

POLL_MESSAGES = 200
REFRESH_INTERVAL = 30
HISTORY_WINDOW = 300  # 5 minutes
DISPLAY_TIMEZONE = "Pacific/Auckland"
DISPLAY_TS_COLS = {
    "detected_at",
    "first_seen",
    "stall_detected_ts",
    "window_start",
    "event_ts",
    "trip_update_time",
    "alert_time",
    "vehicle_time",
}
EPOCH_SECOND_TS_COLS = {"stall_detected_ts", "event_timestamp"}


def _topic(local: str) -> str:
    return local.removeprefix("at.") if CLOUD_MODE else local


st.set_page_config(
    page_title="AT Pipeline — Live",
    page_icon="🚌",
    layout="wide",
)


# --- Kafka helpers ---


def _make_consumer(group_id: str, topics: list[str]) -> Consumer:
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    }
    if CLOUD_MODE:
        config.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": "$ConnectionString",
                "sasl.password": EVENTHUBS_CONNECTION_STRING,
            }
        )
    c = Consumer(config)
    c.subscribe(topics)
    return c


def _poll_messages(
    consumer: Consumer, max_messages: int, timeout: float = 1.0
) -> list[dict]:
    msgs = []
    deadline = time.time() + timeout
    while len(msgs) < max_messages and time.time() < deadline:
        msg = consumer.poll(0.05)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                st.warning(f"Kafka error: {msg.error()}")
            continue
        try:
            msgs.append(json.loads(msg.value().decode("utf-8")))
        except Exception:
            pass
    return msgs


def _init_consumer(key: str, topics: list[str]) -> Consumer:
    if key not in st.session_state:
        st.session_state[key] = _make_consumer(
            group_id=f"streamlit-dashboard-{key}-{int(time.time())}",
            topics=topics,
        )
    return st.session_state[key]


def _init_buffer(key: str) -> list:
    if key not in st.session_state:
        st.session_state[key] = []
    return st.session_state[key]


def _prune_buffer(buf: list, window_s: int = HISTORY_WINDOW) -> list:
    cutoff = time.time() - window_s
    return [m for m in buf if m.get("_received_at", 0) >= cutoff]


# --- UI helpers ---


def _parse_ts_col(series: pd.Series, col_name: str) -> pd.Series:
    numeric_first = col_name in EPOCH_SECOND_TS_COLS or "detected" in col_name.lower()
    if numeric_first:
        numeric = pd.to_numeric(series, errors="coerce")
        if numeric.notna().any():
            return pd.to_datetime(
                numeric, unit="s", errors="coerce", utc=True
            ).dt.tz_convert(DISPLAY_TIMEZONE)
    parsed = pd.to_datetime(series, errors="coerce")
    if parsed.notna().any():
        if getattr(parsed.dt, "tz", None) is not None:
            return parsed.dt.tz_convert(DISPLAY_TIMEZONE)
        return parsed.dt.tz_localize(DISPLAY_TIMEZONE)
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        return pd.to_datetime(
            numeric, unit="s", errors="coerce", utc=True
        ).dt.tz_convert(DISPLAY_TIMEZONE)
    return parsed


def _display_table(
    rows: list[dict],
    preferred_cols: list[str],
    sort_candidates: list[str],
    dedup_cols: list[str] | None = None,
    rename_map: dict[str, str] | None = None,
) -> None:
    df = pd.DataFrame(rows).copy()
    if dedup_cols:
        dedupe_col = next((c for c in dedup_cols if c in df.columns), None)
        if dedupe_col:
            df = df.drop_duplicates(subset=[dedupe_col], keep="last")
    if rename_map:
        df = df.rename(columns=rename_map)
    for ts_col in DISPLAY_TS_COLS:
        if ts_col in df.columns:
            parsed = _parse_ts_col(df[ts_col], ts_col)
            if parsed.notna().any():
                try:
                    parsed = parsed.dt.tz_localize(None)
                except TypeError:
                    pass
                df[ts_col] = parsed.dt.strftime("%Y-%m-%d %H:%M:%S")
    if "_received_at" in df.columns:
        df["received_at"] = (
            pd.to_datetime(df["_received_at"], unit="s", utc=True)
            .dt.tz_convert(DISPLAY_TIMEZONE)
            .dt.strftime("%Y-%m-%d %H:%M:%S")
        )
    sort_col = next((c for c in sort_candidates if c in df.columns), None)
    if sort_col:
        df = df.sort_values(sort_col, ascending=False, na_position="last")
    show_cols = [c for c in preferred_cols if c in df.columns]
    if "received_at" in df.columns and "received_at" not in show_cols:
        show_cols.append("received_at")
    st.dataframe(df[show_cols].head(50), width="stretch", hide_index=True)


# --- Fetch data ---

alerts_consumer = _init_consumer("alerts", [_topic("at.alerts")])
alerts_buf = _init_buffer("alerts_buf")
new_alerts = _poll_messages(alerts_consumer, POLL_MESSAGES)
now_ts = time.time()
for m in new_alerts:
    m["_received_at"] = now_ts
alerts_buf.extend(new_alerts)
st.session_state["alerts_buf"] = _prune_buffer(alerts_buf)
alerts_buf = st.session_state["alerts_buf"]

hw_consumer = _init_consumer("headway", [_topic("at.headway_metrics")])
hw_buf = _init_buffer("hw_buf")
new_hw = _poll_messages(hw_consumer, POLL_MESSAGES)
for m in new_hw:
    m["_received_at"] = now_ts
hw_buf.extend(new_hw)
st.session_state["hw_buf"] = _prune_buffer(hw_buf)
hw_buf = st.session_state["hw_buf"]

# Classify alerts by job
delay_alerts, stall_events = [], []
for m in alerts_buf:
    if "severity" in m and "delay" in m:
        delay_alerts.append(m)
    elif "stall_duration_s" in m or "reading_count" in m:
        stall_events.append(m)


# Q2: one row per stalled vehicle — the detection code emits a new event each time
# reading_count increments, so raw stall_events overcounts. Keep max reading_count
# snapshot per (vehicle_id, route_id, trip_id) to show current stall state.
def _dedup_stalls(events: list[dict]) -> list[dict]:
    if not events:
        return events
    df = pd.DataFrame(events)
    key_cols = [c for c in ["vehicle_id", "route_id", "trip_id"] if c in df.columns]
    if not key_cols or "reading_count" not in df.columns:
        return events
    # Fill NaN so groupby doesn't silently drop vehicles with no route/trip.
    fill = {c: "" for c in key_cols if c != "vehicle_id"}
    df[list(fill.keys())] = df[list(fill.keys())].fillna("")
    idx = df.groupby(key_cols)["reading_count"].idxmax()
    return df.loc[idx].to_dict("records")


stall_events = _dedup_stalls(stall_events)


# --- Page header ---

st.title("🚌 Auckland Transport — Live Pipeline Dashboard")
st.caption(f"Kafka: `{KAFKA_BOOTSTRAP}` · refreshes every {REFRESH_INTERVAL}s")

# Summary row across all three outputs
c1, c2, c3, c4 = st.columns(4)
c1.metric("Delay Alerts (Q1)", len(delay_alerts), help="Last 5 min")
c2.metric("Stall Events (Q2)", len(stall_events), help="Last 5 min")
if hw_buf:
    df_hw_summary = pd.DataFrame(hw_buf)
    c3.metric(
        "Routes reporting (Q3)",
        int(df_hw_summary.get("route_id", pd.Series()).nunique()),
    )
    # Count unique (route_id, direction_id) pairs flagged as bunching, not window count.
    bunching_mask = df_hw_summary.get("is_bunching", pd.Series(dtype=bool))
    bunching_routes = (
        df_hw_summary[bunching_mask][["route_id", "direction_id"]].drop_duplicates()
        if bunching_mask.any() and "route_id" in df_hw_summary.columns
        else pd.DataFrame()
    )
    c4.metric(
        "Bunching routes (Q3)",
        len(bunching_routes),
        help="Unique (route, direction) pairs in last 5 min",
    )
else:
    c3.metric("Routes reporting (Q3)", "—")
    c4.metric("Bunching events (Q3)", "—")

st.divider()


# ── Map: stall density ────────────────────────────────────────────────────────

st.subheader("🗺 Auckland — Vehicle Stall Density")

_LAT_MIN, _LAT_MAX = -37.20, -36.50
_LON_MIN, _LON_MAX = 174.50, 175.10


def _valid_auckland_coord(lat, lon) -> bool:
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError):
        return False
    return _LAT_MIN <= lat <= _LAT_MAX and _LON_MIN <= lon <= _LON_MAX


map_rows = [
    {
        "lat": float(m["latitude"]),
        "lon": float(m["longitude"]),
        "reading_count": int(m.get("reading_count", 1)),
    }
    for m in stall_events
    if _valid_auckland_coord(m.get("latitude"), m.get("longitude"))
]

if map_rows:
    map_df = pd.DataFrame(map_rows)
    st.pydeck_chart(
        pdk.Deck(
            map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
            initial_view_state=pdk.ViewState(
                latitude=-36.86,
                longitude=174.76,
                zoom=11,
                pitch=0,
            ),
            layers=[
                pdk.Layer(
                    "HeatmapLayer",
                    data=map_df,
                    get_position=["lon", "lat"],
                    get_weight="reading_count",
                    radius_pixels=60,
                    opacity=0.8,
                ),
                pdk.Layer(
                    "ScatterplotLayer",
                    data=map_df,
                    get_position=["lon", "lat"],
                    get_radius=150,
                    get_fill_color=[255, 120, 0, 160],
                    pickable=True,
                ),
            ],
            tooltip={"text": "Reading count: {reading_count}"},
        ),
        height=420,
    )
else:
    st.info("No stall location data to map.")

st.divider()


# ── Q1: Delay Alerts ─────────────────────────────────────────────────────────

st.subheader("Q1 — Delay Alerts")

if delay_alerts:
    df_d = pd.DataFrame(delay_alerts)
    sev_counts = (
        df_d.get("severity", pd.Series())
        .value_counts()
        .reindex(["MODERATE", "HIGH", "SEVERE"], fill_value=0)
    )
    s1, s2, s3 = st.columns(3)
    s1.metric("🟡 MODERATE", int(sev_counts.get("MODERATE", 0)))
    s2.metric("🟠 HIGH", int(sev_counts.get("HIGH", 0)))
    s3.metric("🔴 SEVERE", int(sev_counts.get("SEVERE", 0)))

    f1, f2 = st.columns(2)
    severity_focus = f1.selectbox(
        "Severity filter",
        options=["SEVERE", "HIGH", "MODERATE", "ALL"],
        index=0,
    )
    trip_options = ["ALL"] + sorted(
        df_d.get("trip_id", pd.Series(dtype=str)).dropna().astype(str).unique()
    )
    selected_trip = f2.selectbox("Trip", options=trip_options)

    filtered = delay_alerts
    if severity_focus != "ALL":
        filtered = [m for m in filtered if str(m.get("severity")) == severity_focus]
    if selected_trip != "ALL":
        filtered = [m for m in filtered if str(m.get("trip_id")) == selected_trip]

    _display_table(
        filtered,
        preferred_cols=[
            "alert_id",
            "trip_id",
            "route_id",
            "delay_s",
            "severity",
            "detected_at",
        ],
        sort_candidates=["detected_at", "_received_at"],
        dedup_cols=["trip_id", "alert_id"],
        rename_map={"delay": "delay_s", "event_id": "alert_id"},
    )
else:
    st.info("No delay alerts in the last 5 minutes.")

st.divider()


# ── Q2: Vehicle Stall Events ─────────────────────────────────────────────────

st.subheader("Q2 — Vehicle Stall Events")

if stall_events:
    _display_table(
        stall_events,
        preferred_cols=[
            "stall_id",
            "vehicle_id",
            "route_id",
            "reading_count",
            "first_seen",
            "stall_detected_ts",
            "latitude",
            "longitude",
            "detected_at",
        ],
        sort_candidates=["detected_at", "stall_detected_ts", "_received_at"],
        dedup_cols=["stall_id"],
    )
else:
    st.info("No stall events in the last 5 minutes.")

st.divider()


# ── Q3: Headway Regularity ────────────────────────────────────────────────────

st.subheader("Q3 — Headway Regularity & Bus Bunching")
st.warning(
    "**Note (Phase 3 pending):** Current headway is computed from trip_updates "
    "`start_time + delay` (scheduled departure offset), not VP-based stop arrivals. "
    "Multiple trips with different scheduled departures can fall in the same 10-min "
    "event_ts window, making `headway_mean_s` and `headway_cv` reflect schedule spread "
    "rather than physical bus gaps. Values should not be used as operational signals "
    "until VP migration is complete. `window_start` timestamps are UTC.",
    icon="⚠️",
)

if hw_buf:
    df_hw = pd.DataFrame(hw_buf)
    avg_cv = df_hw.get("headway_cv", pd.Series(dtype=float)).mean()
    st.caption(
        f"Avg headway CV: {avg_cv:.3f}" if pd.notna(avg_cv) else "Avg headway CV: —"
    )

    bunching = (
        df_hw[df_hw.get("is_bunching", False)]
        if "is_bunching" in df_hw.columns
        else pd.DataFrame()
    )
    if not bunching.empty:
        st.markdown("**Routes currently bunching:**")
        show_cols = [
            c
            for c in [
                "route_id",
                "direction_id",
                "headway_cv",
                "headway_mean_s",
                "trip_count",
                "window_start",
            ]
            if c in bunching.columns
        ]
        st.dataframe(
            bunching[show_cols].sort_values("headway_cv", ascending=False).head(20),
            width="stretch",
            hide_index=True,
        )
    else:
        st.success("No bunching detected in the last 5 minutes.")

    if "route_id" in df_hw.columns and "headway_cv" in df_hw.columns:
        top_routes = df_hw["route_id"].value_counts().head(15).index
        chart_df = (
            df_hw[df_hw["route_id"].isin(top_routes)]
            .groupby("route_id")["headway_cv"]
            .mean()
            .dropna()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"headway_cv": "avg_headway_cv"})
        )
        if not chart_df.empty:
            st.markdown("**Headway CV by route (higher = more irregular)**")
            st.bar_chart(chart_df.set_index("route_id")["avg_headway_cv"])
else:
    st.info("No headway metrics received yet. Is Q3 running?")

st.divider()


# ── Raw feed (collapsible) ────────────────────────────────────────────────────

with st.expander("🔍 Raw Kafka messages (last 20 per topic)", expanded=False):
    col_a, col_h = st.columns(2)
    with col_a:
        st.caption(f"`{_topic('at.alerts')}`")
        for m in list(reversed(alerts_buf))[:20]:
            with st.expander(
                f"{m.get('trip_id') or m.get('vehicle_id') or '?'} — "
                f"{m.get('severity') or m.get('stall_duration_s') or ''}",
                expanded=False,
            ):
                st.json({k: v for k, v in m.items() if not k.startswith("_")})
    with col_h:
        st.caption(f"`{_topic('at.headway_metrics')}`")
        for m in list(reversed(hw_buf))[:20]:
            with st.expander(
                f"route {m.get('route_id', '?')} dir {m.get('direction_id', '?')} "
                f"cv={m.get('headway_cv', '—')}",
                expanded=False,
            ):
                st.json({k: v for k, v in m.items() if not k.startswith("_")})


# ── Auto-refresh ──────────────────────────────────────────────────────────────

st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")
time.sleep(REFRESH_INTERVAL)
st.rerun()
