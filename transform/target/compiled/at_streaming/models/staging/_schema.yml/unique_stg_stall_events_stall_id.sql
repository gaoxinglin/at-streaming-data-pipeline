
    
    

select
    stall_id as unique_field,
    count(*) as n_records

from "at_streaming"."main"."stg_stall_events"
where stall_id is not null
group by stall_id
having count(*) > 1


