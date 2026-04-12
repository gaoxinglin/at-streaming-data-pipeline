
    
    

select
    route_id as unique_field,
    count(*) as n_records

from "at_streaming"."main"."dim_routes"
where route_id is not null
group by route_id
having count(*) > 1


