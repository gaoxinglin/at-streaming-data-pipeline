
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select stop_name
from "at_streaming"."main"."stg_gtfs_stops"
where stop_name is null



  
  
      
    ) dbt_internal_test