"""
AT Streaming Pipeline — Real-time Alert Dashboard

Reads from the Bronze DuckDB database (written by Spark streaming jobs)
and displays live delay alerts, vehicle positions, and service alerts.

Run:  uv run streamlit run src/dashboard/app.py
"""

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = "data/at_bronze.duckdb"
REFRESH_INTERVAL_S = 30


# ── page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="AT Pipeline — Live Alerts",
    page_icon="🚌",
    layout="wide",
)

st.title("Auckland Transport — Real-time Alert Dashboard")


# ── database connection ─────────────────────────────────────────────────────


@st.cache_resource
def get_db():
    return duckdb.connect(DB_PATH, read_only=True)


con = get_db()


# ── helper ───────────────────────────────────────────────────────────────────


def query(sql: str) -> pd.DataFrame:
    return con.sql(sql).fetchdf()


# ── sidebar: filters ────────────────────────────────────────────────────────

st.sidebar.header("Filters")

dates = query(
    "SELECT DISTINCT cast(event_ts as date) AS d FROM trip_updates ORDER BY d"
)["d"].tolist()

selected_date = st.sidebar.selectbox(
    "Date", dates, index=len(dates) - 1 if dates else 0
)

severity_options = ["All", "SEVERE", "HIGH", "MODERATE"]
selected_severity = st.sidebar.selectbox("Min severity", severity_options)

auto_refresh = st.sidebar.toggle("Auto-refresh", value=True)
if auto_refresh:
    st.sidebar.caption(f"Refreshes every {REFRESH_INTERVAL_S}s")


# ── KPI row ─────────────────────────────────────────────────────────────────

kpi = query(
    f"""
    SELECT
        count(*) AS total_updates,
        count(*) FILTER (WHERE delay > 300)  AS delay_alerts,
        count(*) FILTER (WHERE delay > 1200) AS severe_alerts,
        round(avg(delay) FILTER (WHERE delay > 300) / 60.0, 1) AS avg_delay_min,
        count(DISTINCT route_id) FILTER (WHERE delay > 300) AS routes_affected
    FROM trip_updates
    WHERE is_deleted = false
      AND cast(event_ts AS date) = '{selected_date}'
"""
)

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Trip Updates", f"{kpi['total_updates'][0]:,}")
k2.metric("Delay Alerts (>5min)", f"{kpi['delay_alerts'][0]:,}")
k3.metric("Severe (>20min)", f"{kpi['severe_alerts'][0]:,}")
k4.metric("Avg Delay", f"{kpi['avg_delay_min'][0]} min")
k5.metric("Routes Affected", f"{kpi['routes_affected'][0]:,}")

st.divider()

# ── tabs ─────────────────────────────────────────────────────────────────────

tab_delays, tab_map, tab_alerts, tab_routes = st.tabs(
    ["⏱ Delay Alerts", "🗺 Vehicle Map", "⚠ Service Alerts", "📊 Route Rankings"]
)

# ── tab 1: delay alerts timeline ────────────────────────────────────────────

with tab_delays:
    severity_filter = ""
    if selected_severity == "SEVERE":
        severity_filter = "AND delay > 1200"
    elif selected_severity == "HIGH":
        severity_filter = "AND delay > 600"
    elif selected_severity == "MODERATE":
        severity_filter = "AND delay > 300"

    hourly = query(
        f"""
        SELECT
            extract(hour FROM event_ts) AS hour,
            CASE
                WHEN delay > 1200 THEN 'SEVERE (>20min)'
                WHEN delay > 600  THEN 'HIGH (10-20min)'
                ELSE 'MODERATE (5-10min)'
            END AS severity,
            count(*) AS alert_count
        FROM trip_updates
        WHERE is_deleted = false
          AND delay > 300
          AND cast(event_ts AS date) = '{selected_date}'
          {severity_filter}
        GROUP BY 1, 2
        ORDER BY 1
    """
    )

    if not hourly.empty:
        fig = px.bar(
            hourly,
            x="hour",
            y="alert_count",
            color="severity",
            color_discrete_map={
                "SEVERE (>20min)": "#dc3545",
                "HIGH (10-20min)": "#fd7e14",
                "MODERATE (5-10min)": "#ffc107",
            },
            labels={"hour": "Hour of Day", "alert_count": "Alert Count"},
            title="Delay Alerts by Hour",
        )
        fig.update_layout(bargap=0.1)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No delay alerts for the selected filters.")

    # recent alerts table
    st.subheader("Recent Delay Alerts")
    recent = query(
        f"""
        SELECT
            event_ts,
            route_id,
            trip_id,
            round(delay / 60.0, 1) AS delay_min,
            CASE
                WHEN delay > 1200 THEN '🔴 SEVERE'
                WHEN delay > 600  THEN '🟠 HIGH'
                ELSE '🟡 MODERATE'
            END AS severity
        FROM trip_updates
        WHERE is_deleted = false
          AND delay > 300
          AND cast(event_ts AS date) = '{selected_date}'
          {severity_filter}
        ORDER BY event_ts DESC
        LIMIT 50
    """
    )
    st.dataframe(recent, use_container_width=True, hide_index=True)

# ── tab 2: vehicle map ──────────────────────────────────────────────────────

with tab_map:
    st.subheader("Latest Vehicle Positions")
    st.caption("Most recent GPS ping per vehicle")

    # get latest position per vehicle — use a window to pick the freshest row
    vehicles = query(
        f"""
        WITH latest AS (
            SELECT *,
                row_number() OVER (PARTITION BY vehicle_id ORDER BY event_ts DESC) AS rn
            FROM vehicle_positions
            WHERE cast(event_ts AS date) = '{selected_date}'
              AND latitude BETWEEN -37.2 AND -36.4
              AND longitude BETWEEN 174.4 AND 175.2
        )
        SELECT vehicle_id, route_id, latitude, longitude, speed,
               current_status, event_ts
        FROM latest
        WHERE rn = 1
    """
    )

    if not vehicles.empty:
        fig_map = px.scatter_map(
            vehicles,
            lat="latitude",
            lon="longitude",
            color="current_status",
            hover_data=["vehicle_id", "route_id", "speed", "event_ts"],
            zoom=10,
            height=600,
            title=f"{len(vehicles)} vehicles — last known position",
        )
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.info("No vehicle position data for the selected date.")

# ── tab 3: service alerts ───────────────────────────────────────────────────

with tab_alerts:
    st.subheader("Active Service Alerts")

    svc_alerts = query(
        f"""
        WITH latest AS (
            SELECT *,
                row_number() OVER (PARTITION BY alert_id ORDER BY event_ts DESC) AS rn
            FROM service_alerts
            WHERE cast(event_ts AS date) = '{selected_date}'
        )
        SELECT
            alert_id,
            route_id,
            cause,
            effect,
            header_text,
            active_period_start,
            active_period_end,
            event_ts
        FROM latest WHERE rn = 1
        ORDER BY event_ts DESC
    """
    )

    if not svc_alerts.empty:
        # summary counts by cause
        cause_counts = svc_alerts.groupby("cause").size().reset_index(name="count")
        col_chart, col_table = st.columns([1, 2])

        with col_chart:
            fig_cause = px.pie(
                cause_counts,
                values="count",
                names="cause",
                title="Alerts by Cause",
            )
            st.plotly_chart(fig_cause, use_container_width=True)

        with col_table:
            st.dataframe(
                svc_alerts[["route_id", "cause", "effect", "header_text", "event_ts"]],
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("No service alerts for the selected date.")

# ── tab 4: route rankings ───────────────────────────────────────────────────

with tab_routes:
    st.subheader("Worst Routes by Delay")

    worst_routes = query(
        f"""
        SELECT
            route_id,
            count(*) FILTER (WHERE delay > 300) AS alert_count,
            round(avg(delay) FILTER (WHERE delay > 300) / 60.0, 1) AS avg_delay_min,
            round(max(delay) / 60.0, 1) AS max_delay_min,
            count(DISTINCT trip_id) FILTER (WHERE delay > 300) AS trips_affected
        FROM trip_updates
        WHERE is_deleted = false
          AND cast(event_ts AS date) = '{selected_date}'
        GROUP BY route_id
        HAVING count(*) FILTER (WHERE delay > 300) > 0
        ORDER BY alert_count DESC
        LIMIT 20
    """
    )

    if not worst_routes.empty:
        fig_routes = px.bar(
            worst_routes,
            x="route_id",
            y="alert_count",
            color="avg_delay_min",
            color_continuous_scale="YlOrRd",
            labels={
                "route_id": "Route",
                "alert_count": "Alert Count",
                "avg_delay_min": "Avg Delay (min)",
            },
            title="Top 20 Routes by Delay Alert Count",
        )
        st.plotly_chart(fig_routes, use_container_width=True)

        st.dataframe(worst_routes, use_container_width=True, hide_index=True)
    else:
        st.info("No delays recorded for the selected date.")


# ── auto-refresh ─────────────────────────────────────────────────────────────

if auto_refresh:
    import time

    time.sleep(REFRESH_INTERVAL_S)
    st.rerun()
