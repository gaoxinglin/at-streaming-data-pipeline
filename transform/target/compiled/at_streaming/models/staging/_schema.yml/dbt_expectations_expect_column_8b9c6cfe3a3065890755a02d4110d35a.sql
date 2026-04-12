






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and reading_count >= 3
)
 as expression


    from "at_streaming"."main"."stg_stall_events"
    

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







