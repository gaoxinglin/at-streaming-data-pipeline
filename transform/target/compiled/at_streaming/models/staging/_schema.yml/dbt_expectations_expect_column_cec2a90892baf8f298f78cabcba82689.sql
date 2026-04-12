






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and trips_affected >= 0
)
 as expression


    from "at_streaming"."main"."stg_alert_correlations"
    

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







