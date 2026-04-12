
  
  create view "at_streaming"."main"."stg_gtfs_stops__dbt_tmp" as (
    -- Clean GTFS static stops. Validates coordinates are within NZ bounding box.

with source as (
    select * from 
    
        "at_streaming"."main"."gtfs_stops"
    

),

cleaned as (
    select
        stop_id,
        stop_name,
        stop_lat,
        stop_lon,
        stop_code
    from source
    where stop_id is not null
      and stop_lat between -48 and -34  -- NZ latitude range
      and stop_lon between 166 and 179  -- NZ longitude range
)

select * from cleaned
  );
