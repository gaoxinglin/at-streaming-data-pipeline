






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and avg_headway_s >= 0
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







