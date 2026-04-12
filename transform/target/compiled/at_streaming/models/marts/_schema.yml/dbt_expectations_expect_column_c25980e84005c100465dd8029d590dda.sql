






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and trips_affected >= 0 and trips_affected <= 500
)
 as expression


    from "at_streaming"."main"."fct_alert_impact"
    

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







