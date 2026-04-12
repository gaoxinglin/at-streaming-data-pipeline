






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and headway_mean_s >= 0
)
 as expression


    from "at_streaming"."main"."stg_headway_metrics"
    

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







