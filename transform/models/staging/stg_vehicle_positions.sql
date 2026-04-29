-- Clean vehicle GPS pings from Bronze.
-- Deduplicates on (vehicle_id, event_ts): same reasoning as trip_updates —
-- the AT API re-sends the same GPS ping every ~30s while the vehicle hasn't moved.
-- Drops debug columns (_raw_payload), validates coordinates, renames speed.

WITH source AS (
    SELECT * FROM {{ read_bronze('vehicle_positions') }}
),

deduped AS (
    SELECT *
    FROM source
    QUALIFY row_number() OVER (
        PARTITION BY vehicle_id, event_ts
        ORDER BY ingested_at
    ) = 1
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
        cast(convert_timezone('UTC', 'Pacific/Auckland', event_ts) AS date) AS event_date,
        extract(HOUR FROM convert_timezone('UTC', 'Pacific/Auckland', event_ts)) AS event_hour
    FROM deduped
    WHERE
        latitude BETWEEN -90 AND 90
        AND longitude BETWEEN -180 AND 180
)

SELECT * FROM cleaned
