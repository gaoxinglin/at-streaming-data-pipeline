-- Clean headway regularity metrics from Spark Q3 output.
-- Passes through window-level aggregates, adds event_date/hour.

WITH source AS (
    SELECT * FROM {{ read_bronze('headway_metrics') }}
),

cleaned AS (
    SELECT
        route_id,
        direction_id,
        window_start,
        window_end,
        trip_count,
        headway_mean_s,
        headway_stddev_s,
        headway_cv,
        is_bunching,
        cast(window_start AS date) AS event_date,
        extract(HOUR FROM window_start) AS event_hour
    FROM source
    WHERE trip_count > 0
)

SELECT * FROM cleaned
