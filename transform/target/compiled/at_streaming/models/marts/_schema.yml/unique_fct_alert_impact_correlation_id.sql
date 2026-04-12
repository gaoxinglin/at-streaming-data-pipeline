
    
    

select
    correlation_id as unique_field,
    count(*) as n_records

from "at_streaming"."main"."fct_alert_impact"
where correlation_id is not null
group by correlation_id
having count(*) > 1


