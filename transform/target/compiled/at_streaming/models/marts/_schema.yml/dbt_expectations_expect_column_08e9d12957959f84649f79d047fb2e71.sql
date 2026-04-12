






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and bunching_pct >= 0 and bunching_pct <= 100
)
 as expression


    from "at_streaming"."main"."fct_headway_regularity"
    

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







