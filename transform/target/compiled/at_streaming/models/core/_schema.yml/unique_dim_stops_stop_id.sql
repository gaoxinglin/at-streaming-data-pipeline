
    
    

select
    stop_id as unique_field,
    count(*) as n_records

from "at_streaming"."main"."dim_stops"
where stop_id is not null
group by stop_id
having count(*) > 1


