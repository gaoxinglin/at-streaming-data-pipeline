{#
    Cross-platform Bronze source reader.
    - duckdb: read_parquet() from local Parquet files written by Spark
    - databricks: standard source() reference to catalog tables
#}

{% macro read_bronze(table_name) %}
    {% if target.type == 'duckdb' %}
        read_parquet('{{ env_var("BRONZE_OUTPUT_PATH", "data/bronze") }}/{{ table_name }}/**/*.parquet', hive_partitioning=true)
    {% else %}
        {{ source('bronze', table_name) }}
    {% endif %}
{% endmacro %}
