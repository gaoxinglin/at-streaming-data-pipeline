{#
    Cross-platform GTFS static reader.
    - duckdb: dbt seed tables (loaded from seeds/*.csv)
    - databricks: source tables loaded by Databricks Workflow into Delta
#}

{% macro read_gtfs(table_name) %}
    {{ ref('gtfs_' ~ table_name) }}
{% endmacro %}
