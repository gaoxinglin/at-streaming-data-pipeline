-- Clean trip delay updates from Bronze.
-- Filters out deleted records, adds delay_minutes.

with source as (
    select * from 
    
        read_parquet('data/bronze/trip_updates/**/*.parquet', hive_partitioning=true)
    

),

cleaned as (
    select
        event_id,
        source_id,
        trip_id,
        route_id,
        direction_id,
        start_time,
        start_date,
        delay,
        round(delay / 60.0, 2) as delay_minutes,
        event_ts,
        cast(event_ts as date) as event_date,
        extract(hour from event_ts) as event_hour
    from source
    where is_deleted = false
)

select * from cleaned