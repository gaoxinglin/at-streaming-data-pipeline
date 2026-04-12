






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and delay >= 300 and delay <= 7200
)
 as expression


    from "at_streaming"."main"."fct_delay_alerts"
    

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







