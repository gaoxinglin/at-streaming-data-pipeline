-- Q3 Historical: hourly headway regularity by route.
-- Rolls up Spark Q3 sliding-window metrics into hourly buckets for trend analysis.

{%- set gold_path = env_var('GOLD_ADLS_PATH', '') -%}
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        location_root=gold_path or none
    )
}}

WITH hourly AS (
    SELECT
        h.route_id,
        r.route_name,
        r.route_type_label,
        date_trunc('hour', h.window_start) AS hour_bucket,
        round(avg(h.headway_mean_s), 1) AS avg_headway_s,
        round(avg(h.headway_cv), 3) AS headway_cv,
        round(
            100.0 * sum(CASE WHEN h.is_bunching THEN 1 ELSE 0 END)
            / count(*), 1
        ) AS bunching_pct,
        sum(h.trip_count) AS trip_count
    FROM {{ ref('stg_headway_metrics') }} h
    LEFT JOIN {{ ref('dim_routes') }} r USING (route_id)
    {% if is_incremental() %}
        WHERE h.event_date > (SELECT max(cast(hour_bucket AS date)) FROM {{ this }})
    {% endif %}
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM hourly
