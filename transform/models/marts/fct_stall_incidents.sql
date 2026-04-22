-- Q2 Historical: vehicle stall incidents with location and duration.
-- Reads from Spark Q2 stall detection output, enriches with route name.

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='stall_id'
    )
}}

WITH stalls AS (
    SELECT
        stall_id,
        event_date,
        event_hour,
        vehicle_id,
        route_id,
        latitude,
        longitude,
        stall_duration_s,
        detected_at
    FROM {{ ref('stg_stall_events') }}
    {% if is_incremental() %}
        WHERE event_date > (SELECT max(event_date) FROM {{ this }})
    {% endif %}
)

SELECT
    s.stall_id,
    s.event_date,
    s.event_hour,
    s.vehicle_id,
    s.route_id,
    r.route_name,
    s.latitude,
    s.longitude,
    s.stall_duration_s,
    s.detected_at
FROM stalls s
LEFT JOIN {{ ref('dim_routes') }} r USING (route_id)
