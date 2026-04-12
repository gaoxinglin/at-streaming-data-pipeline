-- Clean GTFS static routes. Maps route_type code to human-readable label.
-- GTFS spec: 2 = Rail, 3 = Bus (Auckland uses these two)

with source as (
    select * from {{ read_gtfs('routes') }}
),

cleaned as (
    select
        route_id,
        route_short_name,
        route_long_name,
        route_type,
        case route_type
            when 2 then 'Rail'
            when 3 then 'Bus'
            else 'Other'
        end as route_type_label
    from source
    where route_id is not null
)

select * from cleaned
