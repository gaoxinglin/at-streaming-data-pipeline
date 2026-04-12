
    
    

select
    correlation_id as unique_field,
    count(*) as n_records

from "at_streaming"."main"."stg_alert_correlations"
where correlation_id is not null
group by correlation_id
having count(*) > 1


