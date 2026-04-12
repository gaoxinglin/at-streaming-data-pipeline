
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    stop_id as unique_field,
    count(*) as n_records

from "at_streaming"."main"."stg_gtfs_stops"
where stop_id is not null
group by stop_id
having count(*) > 1



  
  
      
    ) dbt_internal_test