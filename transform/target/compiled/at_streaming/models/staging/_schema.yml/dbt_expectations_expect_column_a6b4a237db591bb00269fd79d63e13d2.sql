






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and stop_lon >= 166 and stop_lon <= 179
)
 as expression


    from "at_streaming"."main"."stg_gtfs_stops"
    

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







