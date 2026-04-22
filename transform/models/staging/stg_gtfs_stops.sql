-- Clean GTFS static stops. Validates coordinates are within NZ bounding box.

WITH source AS (
    SELECT * FROM {{ read_gtfs('stops') }}
),

cleaned AS (
    SELECT
        stop_id,
        stop_name,
        stop_lat,
        stop_lon,
        stop_code
    FROM source
    WHERE
        stop_id IS NOT null
        AND stop_lat BETWEEN -48 AND -34  -- NZ latitude range
        AND stop_lon BETWEEN 166 AND 179  -- NZ longitude range
)

SELECT * FROM cleaned
