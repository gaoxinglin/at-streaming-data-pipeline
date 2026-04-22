-- Stop dimension: joins downstream on stop_id to enrich facts with name + location.
-- Materialized as table — ~7k stops, refreshed weekly by Databricks Workflow.

SELECT
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    stop_code
FROM {{ ref('stg_gtfs_stops') }}
