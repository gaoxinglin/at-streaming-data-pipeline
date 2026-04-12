{#
    Cross-platform GTFS static reader.
    - duckdb: dbt seed tables (loaded from seeds/*.csv)
    - databricks: source tables loaded by Airflow into Delta
#}

{% macro read_gtfs(table_name) %}
    {% if target.type == 'duckdb' %}
        {{ ref('gtfs_' ~ table_name) }}
    {% else %}
        {{ source('gtfs_static', table_name) }}
    {% endif %}
{% endmacro %}
