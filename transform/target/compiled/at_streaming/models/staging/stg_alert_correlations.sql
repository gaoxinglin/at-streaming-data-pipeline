-- Clean alert correlation results from Spark Q4 output.
-- Passes through impact aggregates from the 3-stream join.

with source as (
    select * from 
    
        read_parquet('data/bronze/alert_correlations/**/*.parquet', hive_partitioning=true)
    

),

cleaned as (
    select
        correlation_id,
        alert_id,
        route_id,
        vehicles_affected,
        trips_affected,
        avg_delay_at_time,
        detected_at,
        event_date,
        extract(hour from detected_at) as event_hour
    from source
    where vehicles_affected > 0 or trips_affected > 0
)

select * from cleaned