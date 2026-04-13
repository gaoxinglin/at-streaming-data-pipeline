
  
  create view "at_streaming"."main"."stg_vehicle_positions__dbt_tmp" as (
    -- Clean vehicle GPS pings from Bronze.
-- Drops debug columns (_raw_payload), validates coordinates, renames speed.

with source as (
    select * from 
    
        read_parquet('data/bronze/vehicle_positions/**/*.parquet', hive_partitioning=true)
    

),

cleaned as (
    select
        event_id,
        vehicle_id,
        trip_id,
        route_id,
        latitude,
        longitude,
        speed as speed_kmh,
        current_stop_sequence,
        stop_id,
        current_status,
        event_ts,
        cast(event_ts as date) as event_date,
        extract(hour from event_ts) as event_hour
    from source
    where latitude between -90 and 90
      and longitude between -180 and 180
)

select * from cleaned
  );
