-- Q3 Historical: hourly headway regularity by route.
-- Rolls up Spark Q3 sliding-window metrics into hourly buckets for trend analysis.

{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

with hourly as (
    select
        h.route_id,
        r.route_name,
        r.route_type_label,
        date_trunc('hour', h.window_start) as hour_bucket,
        round(avg(h.headway_mean_s), 1) as avg_headway_s,
        round(avg(h.headway_cv), 3) as headway_cv,
        round(
            100.0 * sum(case when h.is_bunching then 1 else 0 end)
            / count(*), 1
        ) as bunching_pct,
        sum(h.trip_count) as trip_count
    from {{ ref('stg_headway_metrics') }} as h
    left join {{ ref('dim_routes') }} as r on h.route_id = r.route_id
    {% if is_incremental() %}
    where h.event_date > (select max(cast(hour_bucket as date)) from {{ this }})
    {% endif %}
    group by 1, 2, 3, 4
)

select * from hourly
