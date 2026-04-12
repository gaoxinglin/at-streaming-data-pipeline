
    
    

select
    event_id as unique_field,
    count(*) as n_records

from "at_streaming"."main"."stg_vehicle_positions"
where event_id is not null
group by event_id
having count(*) > 1


