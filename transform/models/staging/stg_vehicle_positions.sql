-- Clean vehicle GPS pings from Bronze.
-- Drops debug columns (_raw_payload), validates coordinates, renames speed.

WITH source AS (
    SELECT * FROM {{ read_bronze('vehicle_positions') }}
),

cleaned AS (
    SELECT
        event_id,
        vehicle_id,
        trip_id,
        route_id,
        latitude,
        longitude,
        speed AS speed_kmh,
        current_stop_sequence,
        stop_id,
        current_status,
        event_ts,
        cast(event_ts AS date) AS event_date,
        extract(HOUR FROM event_ts) AS event_hour
    FROM source
    WHERE
        latitude BETWEEN -90 AND 90
        AND longitude BETWEEN -180 AND 180
)

SELECT * FROM cleaned
