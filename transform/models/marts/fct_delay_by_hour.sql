-- Hourly delay summary across all trips (not just delayed ones).
-- Used by the "Delay by hour" dashboard chart. Including on-time trips (delay~=0)
-- is what gives the natural rush-hour pattern — averaging only delayed trips
-- produces a flat ~9 min line across all hours.

{%- set gold_path = env_var('GOLD_ADLS_PATH', '') -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        location_root=gold_path or none
    )
}}

SELECT
    event_date,
    event_hour,
    round(avg(delay_minutes), 2) AS avg_delay_minutes,
    round(avg(CASE WHEN delay > 0 THEN delay_minutes END), 2) AS avg_delay_minutes_delayed_only,
    count(*) AS trip_count,
    sum(CASE WHEN delay > 300 THEN 1 ELSE 0 END) AS delayed_trip_count
FROM {{ ref('stg_trip_updates') }}
{% if is_incremental() %}
    WHERE event_date > (SELECT max(event_date) FROM {{ this }})
{% endif %}
GROUP BY event_date, event_hour
