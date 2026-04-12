
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select route_name
from "at_streaming"."main"."dim_routes"
where route_name is null



  
  
      
    ) dbt_internal_test