-- Q2 Historical: vehicle stall incidents with location and duration.
-- Reads from Spark Q2 stall detection output, enriches with route name.

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='stall_id'
    )
}}

with stalls as (
    select
        stall_id,
        event_date,
        event_hour,
        vehicle_id,
        route_id,
        latitude,
        longitude,
        stall_duration_s,
        detected_at
    from {{ ref('stg_stall_events') }}
    {% if is_incremental() %}
    where event_date > (select max(event_date) from {{ this }})
    {% endif %}
)

select
    s.stall_id,
    s.event_date,
    s.event_hour,
    s.vehicle_id,
    s.route_id,
    r.route_name,
    s.latitude,
    s.longitude,
    s.stall_duration_s,
    s.detected_at
from stalls as s
left join {{ ref('dim_routes') }} as r on s.route_id = r.route_id
