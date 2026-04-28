-- Q1 Historical: delay alert events by route.
-- Filters trip_updates where delay > 5 min and enriches with route dimension.
-- This is the dbt counterpart to the real-time Spark delay_alert_job.

{%- set gold_path = env_var('GOLD_ADLS_PATH', '') -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='event_id',
        location_root=gold_path or none
    )
}}

WITH delays AS (
    SELECT
        event_id,
        event_date,
        event_hour,
        route_id,
        trip_id,
        delay,
        delay_minutes,
        event_ts
    FROM {{ ref('stg_trip_updates') }}
    WHERE
        delay > 300  -- 5 minutes in seconds
        {% if is_incremental() %}
            AND event_date > (SELECT max(event_date) FROM {{ this }})
        {% endif %}
)

SELECT
    d.event_id,
    d.event_date,
    d.event_hour,
    d.route_id,
    r.route_name,
    r.route_type_label,
    d.trip_id,
    d.delay,
    d.delay_minutes,
    d.event_ts
FROM delays d
LEFT JOIN {{ ref('dim_routes') }} r USING (route_id)
