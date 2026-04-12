-- Q2 Historical: vehicle stall incidents with location and duration.
-- Reads from Spark Q2 stall detection output, enriches with route name.



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
    from "at_streaming"."main"."stg_stall_events"
    
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
from stalls s
left join "at_streaming"."main"."dim_routes" r using (route_id)