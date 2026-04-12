






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and stall_duration_s >= 0 and stall_duration_s <= 86400
)
 as expression


    from "at_streaming"."main"."fct_stall_incidents"
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







