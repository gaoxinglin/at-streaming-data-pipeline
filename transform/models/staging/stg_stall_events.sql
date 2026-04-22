-- Clean stall events from Spark Q2 output.
-- Converts stall_detected_ts (unix epoch) to proper timestamp, computes duration.

WITH source AS (
    SELECT * FROM {{ read_bronze('stall_events') }}
),

cleaned AS (
    SELECT
        stall_id,
        vehicle_id,
        route_id,
        latitude,
        longitude,
        reading_count,
        first_seen,
        -- stall_detected_ts is a unix epoch long in Bronze
        cast(to_timestamp(stall_detected_ts) AS timestamp) AS stall_detected_at,
        -- approximate stall duration: last detection - first seen
        unix_timestamp(cast(to_timestamp(stall_detected_ts) AS timestamp))
        - unix_timestamp(first_seen) AS stall_duration_s,
        detected_at,
        event_date,
        extract(HOUR FROM first_seen) AS event_hour
    FROM source
)

SELECT * FROM cleaned
