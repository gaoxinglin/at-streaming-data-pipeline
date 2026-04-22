-- Route dimension: joins downstream on route_id to enrich facts with name + type.
-- Materialized as table — small dataset (~100 routes), refreshed weekly by Databricks Workflow.

SELECT
    route_id,
    route_short_name AS route_name,
    route_long_name,
    route_type,
    route_type_label
FROM {{ ref('stg_gtfs_routes') }}
