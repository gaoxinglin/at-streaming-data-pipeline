-- Clean trip delay updates from Bronze.
-- Filters out deleted records, adds delay_minutes.

WITH source AS (
    SELECT * FROM {{ read_bronze('trip_updates') }}
),

cleaned AS (
    SELECT
        event_id,
        source_id,
        trip_id,
        route_id,
        direction_id,
        start_time,
        start_date,
        delay,
        round(delay / 60.0, 2) AS delay_minutes,
        event_ts,
        cast(event_ts AS date) AS event_date,
        extract(HOUR FROM event_ts) AS event_hour
    FROM source
    WHERE is_deleted = false
)

SELECT * FROM cleaned
