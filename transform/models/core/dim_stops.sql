-- Stop dimension: joins downstream on stop_id to enrich facts with name + location.
-- Materialized as table — ~7k stops, refreshed weekly by Databricks Workflow.

{%- set gold_path = env_var('GOLD_ADLS_PATH', '') -%}
{{ config(materialized='table', location_root=gold_path or none) }}

SELECT
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    stop_code
FROM {{ ref('stg_gtfs_stops') }}
