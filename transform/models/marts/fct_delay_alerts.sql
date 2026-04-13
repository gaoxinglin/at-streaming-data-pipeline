-- Q1 Historical: delay alert events by route.
-- Filters trip_updates where delay > 5 min and enriches with route dimension.
-- This is the dbt counterpart to the real-time Spark delay_alert_job.

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='event_id'
    )
}}

with delays as (
    select
        event_id,
        event_date,
        event_hour,
        route_id,
        trip_id,
        delay,
        delay_minutes,
        event_ts
    from {{ ref('stg_trip_updates') }}
    where delay > 300  -- 5 minutes in seconds
    {% if is_incremental() %}
      and event_date > (select max(event_date) from {{ this }})
    {% endif %}
)

select
    d.event_id,
    d.event_date,
    d.event_hour,
    d.route_id,
    r.route_name,
    r.route_type_label,
    d.trip_id,
    d.delay,
    d.delay_minutes,
    d.event_ts
from delays as d
left join {{ ref('dim_routes') }} as r on d.route_id = r.route_id
