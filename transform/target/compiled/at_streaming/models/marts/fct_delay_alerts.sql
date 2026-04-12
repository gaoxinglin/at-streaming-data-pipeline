-- Q1 Historical: delay alert events by route.
-- Filters trip_updates where delay > 5 min and enriches with route dimension.
-- This is the dbt counterpart to the real-time Spark delay_alert_job.



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
    from "at_streaming"."main"."stg_trip_updates"
    where delay > 300  -- 5 minutes in seconds
    
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
from delays d
left join "at_streaming"."main"."dim_routes" r using (route_id)