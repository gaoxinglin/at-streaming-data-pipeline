-- Clean stall events from Spark Q2 output.
-- Converts stall_detected_ts (unix epoch) to proper timestamp, computes duration.

with source as (
    select * from 
    
        read_parquet('data/bronze/stall_events/**/*.parquet', hive_partitioning=true)
    

),

cleaned as (
    select
        stall_id,
        vehicle_id,
        route_id,
        latitude,
        longitude,
        reading_count,
        first_seen,
        -- stall_detected_ts is a unix epoch long in Bronze
        cast(to_timestamp(stall_detected_ts) as timestamp) as stall_detected_at,
        -- approximate stall duration: last detection - first seen
        epoch(cast(to_timestamp(stall_detected_ts) as timestamp)) - epoch(first_seen) as stall_duration_s,
        detected_at,
        event_date,
        extract(hour from first_seen) as event_hour
    from source
)

select * from cleaned