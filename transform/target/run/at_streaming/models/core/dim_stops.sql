
  
    
    

    create  table
      "at_streaming"."main"."dim_stops__dbt_tmp"
  
    as (
      -- Stop dimension: joins downstream on stop_id to enrich facts with name + location.
-- Materialized as table — ~7k stops, refreshed weekly by Databricks Workflow.

select
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    stop_code
from "at_streaming"."main"."stg_gtfs_stops"
    );
  
  