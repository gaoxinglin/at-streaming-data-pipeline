
  
  create view "at_streaming"."main"."stg_headway_metrics__dbt_tmp" as (
    -- Clean headway regularity metrics from Spark Q3 output.
-- Passes through window-level aggregates, adds event_date/hour.

with source as (
    select * from 
    
        read_parquet('data/bronze/headway_metrics/**/*.parquet', hive_partitioning=true)
    

),

cleaned as (
    select
        route_id,
        direction_id,
        window_start,
        window_end,
        trip_count,
        headway_mean_s,
        headway_stddev_s,
        headway_cv,
        is_bunching,
        cast(window_start as date) as event_date,
        extract(hour from window_start) as event_hour
    from source
    where trip_count > 0
)

select * from cleaned
  );
