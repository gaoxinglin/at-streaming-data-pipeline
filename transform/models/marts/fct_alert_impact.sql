-- Q4 Historical: service alert impact — affected vehicles, trips, and delays.
-- Joins Spark Q4 correlation output with service_alerts for cause/effect context.

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='correlation_id'
    )
}}

with correlations as (
    select
        correlation_id,
        alert_id,
        route_id,
        vehicles_affected,
        trips_affected,
        avg_delay_at_time as avg_delay_during,
        detected_at,
        event_date
    from {{ ref('stg_alert_correlations') }}
    {% if is_incremental() %}
    where event_date > (select max(event_date) from {{ this }})
    {% endif %}
),

-- get cause/effect from service_alerts bronze (not in correlation output)
alerts as (
    select distinct
        alert_id,
        cause as alert_cause,
        effect as alert_effect
    from {{ read_bronze('service_alerts') }}
)

select
    c.correlation_id,
    c.alert_id,
    c.event_date,
    c.route_id,
    r.route_name,
    a.alert_cause,
    a.alert_effect,
    c.vehicles_affected,
    c.trips_affected,
    c.avg_delay_during,
    c.detected_at
from correlations as c
left join {{ ref('dim_routes') }} as r on c.route_id = r.route_id
left join alerts as a using (alert_id)
