-- Clean trip delay updates from Bronze.
-- Deduplicates on (source_id, event_ts): the AT API returns the same trip update
-- on every poll (~30s) until the trip state changes, so bronze accumulates many
-- identical rows per logical event. We keep the first ingested copy.

WITH source AS (
    SELECT * FROM {{ read_bronze('trip_updates') }}
),

deduped AS (
    SELECT *
    FROM source
    QUALIFY row_number() OVER (
        PARTITION BY source_id, event_ts
        ORDER BY ingested_at
    ) = 1
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
        -- event_ts is UTC; convert to NZ local time for date/hour partitioning
        cast(convert_timezone('UTC', 'Pacific/Auckland', event_ts) AS date) AS event_date,
        extract(HOUR FROM convert_timezone('UTC', 'Pacific/Auckland', event_ts)) AS event_hour
    FROM deduped
    WHERE is_deleted = false
)

SELECT * FROM cleaned
