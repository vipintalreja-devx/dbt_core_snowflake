{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'ref'],
    transient=true
) }}
select
    region_id,
    coalesce(nullif(trim(region_name), ''), 'UNKNOWN')      as region_name,
    city                                                    as region_city,
    coalesce(country_code, 'PH')                            as country_code,
    case
        when region_id in (1, 3)                                          then 'NCR'
        when region_name in ('Central Visayas', 'Western Visayas', 'Eastern Visayas', 'Cebu Metro') then 'VISAYAS'
        when region_name in ('Davao Region', 'Northern Mindanao', 'SOCCSKSARGEN', 'Zamboanga Peninsula', 'Caraga', 'BARMM') then 'MINDANAO'
        when region_name is null                                          then 'UNKNOWN'
        else 'LUZON'
    end                                                     as island_group
from {{ source('ref', 'geo_region') }}
