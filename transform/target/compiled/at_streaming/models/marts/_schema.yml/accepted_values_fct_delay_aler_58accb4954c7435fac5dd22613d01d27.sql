
    
    

with all_values as (

    select
        route_type_label as value_field,
        count(*) as n_records

    from "at_streaming"."main"."fct_delay_alerts"
    group by route_type_label

)

select *
from all_values
where value_field not in (
    'Bus','Rail','Other'
)


