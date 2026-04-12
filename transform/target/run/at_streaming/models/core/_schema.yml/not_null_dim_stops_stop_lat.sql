
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select stop_lat
from "at_streaming"."main"."dim_stops"
where stop_lat is null



  
  
      
    ) dbt_internal_test