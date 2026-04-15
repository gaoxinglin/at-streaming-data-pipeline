"""
Real-time dashboard for AT Streaming Pipeline Q1-Q4.

Reads directly from Kafka topics:
  - at.alerts       → Q1 delay alerts, Q2 stall events, Q4 correlation alerts
  - at.headway_metrics → Q3 headway regularity / bus bunching

Run with:
    streamlit run src/dashboard/app.py
"""

import json
import os
import time
from collections import defaultdict
from datetime import datetime

import pandas as pd
import streamlit as st
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POLL_MESSAGES = 200      # max messages to pull per refresh
REFRESH_INTERVAL = 30     # seconds between auto-refresh
HISTORY_WINDOW = 300     # keep last 5 minutes of messages in session state

st.set_page_config(
    page_title="AT Pipeline — Live",
    page_icon="🚌",
    layout="wide",
)


# --- Kafka helpers ---

def _make_consumer(group_id: str, topics: list[str]) -> Consumer:
    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    c.subscribe(topics)
    return c


def _poll_messages(consumer: Consumer, max_messages: int, timeout: float = 1.0) -> list[dict]:
    """Pull up to max_messages from Kafka, return as parsed dicts."""
    msgs = []
    deadline = time.time() + timeout
    while len(msgs) < max_messages and time.time() < deadline:
        msg = consumer.poll(0.05)
        if msg is None:
            break
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
    """Get or create a cached consumer in session state."""
    if key not in st.session_state:
        st.session_state[key] = _make_consumer(
            group_id=f"streamlit-dashboard-{key}-{int(time.time())}",
            topics=topics,
        )
    return st.session_state[key]


def _init_buffer(key: str) -> list:
    """Get or create a message buffer in session state."""
    if key not in st.session_state:
        st.session_state[key] = []
    return st.session_state[key]


def _prune_buffer(buf: list, window_s: int = HISTORY_WINDOW) -> list:
    """Drop messages older than window_s seconds."""
    cutoff = time.time() - window_s
    return [m for m in buf if m.get("_received_at", 0) >= cutoff]


# --- UI helpers ---

def _severity_color(severity: str) -> str:
    return {"MODERATE": "🟡", "HIGH": "🟠", "SEVERE": "🔴"}.get(severity, "⚪")


# --- Main app ---

st.title("🚌 Auckland Transport — Live Pipeline Dashboard")
st.caption(f"Kafka: `{KAFKA_BOOTSTRAP}` · refreshes every {REFRESH_INTERVAL}s")

tab_alerts, tab_headway, tab_raw = st.tabs(["🚨 Alerts (Q1 / Q2 / Q4)", "📊 Headway Q3", "🔍 Raw feed"])

# ── Fetch at.alerts ──────────────────────────────────────────────────────────
alerts_consumer = _init_consumer("alerts", ["at.alerts"])
alerts_buf = _init_buffer("alerts_buf")

new_alerts = _poll_messages(alerts_consumer, POLL_MESSAGES)
now_ts = time.time()
for m in new_alerts:
    m["_received_at"] = now_ts
alerts_buf.extend(new_alerts)
st.session_state["alerts_buf"] = _prune_buffer(alerts_buf)
alerts_buf = st.session_state["alerts_buf"]

# ── Fetch at.headway_metrics ─────────────────────────────────────────────────
hw_consumer = _init_consumer("headway", ["at.headway_metrics"])
hw_buf = _init_buffer("hw_buf")

new_hw = _poll_messages(hw_consumer, POLL_MESSAGES)
for m in new_hw:
    m["_received_at"] = now_ts
hw_buf.extend(new_hw)
st.session_state["hw_buf"] = _prune_buffer(hw_buf)
hw_buf = st.session_state["hw_buf"]

# ── TAB 1: Alerts ─────────────────────────────────────────────────────────────
with tab_alerts:
    # classify messages by which job produced them
    delay_alerts, stall_events, corr_alerts, unknown = [], [], [], []
    for m in alerts_buf:
        if "severity" in m and "delay" in m:           # Q1
            delay_alerts.append(m)
        elif "stall_duration_s" in m or "reading_count" in m:  # Q2
            stall_events.append(m)
        elif "vehicles_affected" in m or "alert_cause" in m:   # Q4
            corr_alerts.append(m)
        else:
            unknown.append(m)

    col1, col2, col3 = st.columns(3)
    col1.metric("Delay Alerts (Q1)", len(delay_alerts), help="Last 5 min")
    col2.metric("Stall Events (Q2)", len(stall_events), help="Last 5 min")
    col3.metric("Corr. Alerts (Q4)", len(corr_alerts), help="Last 5 min")

    st.divider()

    # Q1 — Delay alerts
    st.subheader("Q1 — Delay Alerts")
    if delay_alerts:
        df_d = pd.DataFrame(delay_alerts)
        # severity breakdown
        sev_counts = df_d.get("severity", pd.Series()).value_counts().reindex(
            ["MODERATE", "HIGH", "SEVERE"], fill_value=0
        )
        c1, c2, c3 = st.columns(3)
        c1.metric("🟡 MODERATE", int(sev_counts.get("MODERATE", 0)))
        c2.metric("🟠 HIGH", int(sev_counts.get("HIGH", 0)))
        c3.metric("🔴 SEVERE", int(sev_counts.get("SEVERE", 0)))

        show_cols = [c for c in ["trip_id", "route_id", "delay", "severity", "detected_at"]
                     if c in df_d.columns]
        st.dataframe(
            df_d[show_cols].sort_values("detected_at", ascending=False).head(50),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No delay alerts in the last 5 minutes.")

    st.divider()

    # Q2 — Stall events
    st.subheader("Q2 — Vehicle Stall Events")
    if stall_events:
        df_s = pd.DataFrame(stall_events)
        show_cols = [c for c in ["vehicle_id", "route_id", "stall_duration_s",
                                  "reading_count", "latitude", "longitude", "detected_at"]
                     if c in df_s.columns]
        st.dataframe(
            df_s[show_cols].sort_values("detected_at", ascending=False).head(50),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No stall events in the last 5 minutes.")

    st.divider()

    # Q4 — Correlation alerts
    st.subheader("Q4 — Service Alert Correlations")
    if corr_alerts:
        df_c = pd.DataFrame(corr_alerts)
        show_cols = [c for c in ["route_id", "alert_cause", "alert_effect",
                                  "vehicles_affected", "trips_affected",
                                  "avg_delay_during", "detected_at"]
                     if c in df_c.columns]
        st.dataframe(
            df_c[show_cols].sort_values("detected_at", ascending=False).head(50),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No correlation alerts in the last 5 minutes.")


# ── TAB 2: Headway (Q3) ──────────────────────────────────────────────────────
with tab_headway:
    st.subheader("Q3 — Headway Regularity & Bus Bunching")

    if hw_buf:
        df_hw = pd.DataFrame(hw_buf)

        # summary metrics
        bunching_count = df_hw.get("is_bunching", pd.Series(dtype=bool)).sum()
        total_routes = df_hw.get("route_id", pd.Series()).nunique()
        avg_cv = df_hw.get("headway_cv", pd.Series(dtype=float)).mean()

        c1, c2, c3 = st.columns(3)
        c1.metric("Routes reporting", int(total_routes))
        c2.metric("Bunching events 🚌🚌", int(bunching_count),
                  help="is_bunching=True in last 5 min")
        c3.metric("Avg headway CV", f"{avg_cv:.3f}" if pd.notna(avg_cv) else "—",
                  help="CV > 0.5 → irregular spacing")

        st.divider()

        # bunching routes table
        bunching = df_hw[df_hw.get("is_bunching", False) == True] if "is_bunching" in df_hw.columns else pd.DataFrame()
        if not bunching.empty:
            st.markdown("**Routes currently bunching:**")
            show_cols = [c for c in ["route_id", "direction_id", "headway_cv",
                                      "headway_mean_s", "trip_count", "window_start"]
                         if c in bunching.columns]
            st.dataframe(
                bunching[show_cols].sort_values("headway_cv", ascending=False).head(20),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.success("No bunching detected in the last 5 minutes.")

        st.divider()

        # headway CV chart — top routes by message count
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


# ── TAB 3: Raw feed ──────────────────────────────────────────────────────────
with tab_raw:
    st.subheader("Raw Kafka messages (last 20)")

    col_a, col_h = st.columns(2)
    with col_a:
        st.caption("`at.alerts`")
        for m in list(reversed(alerts_buf))[:20]:
            with st.expander(
                f"{m.get('trip_id') or m.get('vehicle_id') or '?'} — "
                f"{m.get('severity') or m.get('stall_duration_s') or ''}",
                expanded=False,
            ):
                st.json({k: v for k, v in m.items() if not k.startswith("_")})

    with col_h:
        st.caption("`at.headway_metrics`")
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
