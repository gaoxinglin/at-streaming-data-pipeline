-- Clean GTFS static routes. Maps route_type code to human-readable label.
-- GTFS spec: 2 = Rail, 3 = Bus (Auckland uses these two)

WITH source AS (
    SELECT * FROM {{ read_gtfs('routes') }}
),

cleaned AS (
    SELECT
        route_id,
        route_short_name,
        route_long_name,
        route_type,
        CASE route_type
            WHEN 2 THEN 'Rail'
            WHEN 3 THEN 'Bus'
            ELSE 'Other'
        END AS route_type_label
    FROM source
    WHERE route_id IS NOT null
)

SELECT * FROM cleaned
