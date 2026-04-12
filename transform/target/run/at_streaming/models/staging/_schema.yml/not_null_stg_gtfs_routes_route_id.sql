
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select route_id
from "at_streaming"."main"."stg_gtfs_routes"
where route_id is null



  
  
      
    ) dbt_internal_test