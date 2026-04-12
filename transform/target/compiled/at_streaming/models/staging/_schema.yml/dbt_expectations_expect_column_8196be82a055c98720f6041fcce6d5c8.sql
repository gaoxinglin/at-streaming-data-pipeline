






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and latitude >= -90 and latitude <= 90
)
 as expression


    from "at_streaming"."main"."stg_vehicle_positions"
    

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







