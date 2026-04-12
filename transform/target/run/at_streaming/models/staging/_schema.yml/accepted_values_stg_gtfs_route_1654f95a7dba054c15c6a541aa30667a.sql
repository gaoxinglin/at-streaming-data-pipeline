
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        route_type_label as value_field,
        count(*) as n_records

    from "at_streaming"."main"."stg_gtfs_routes"
    group by route_type_label

)

select *
from all_values
where value_field not in (
    'Bus','Rail','Other'
)



  
  
      
    ) dbt_internal_test