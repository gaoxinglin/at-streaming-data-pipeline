-- Stop dimension: joins downstream on stop_id to enrich facts with name + location.
-- Materialized as table — ~7k stops, refreshed weekly by Databricks Workflow.

select
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    stop_code
from {{ ref('stg_gtfs_stops') }}
