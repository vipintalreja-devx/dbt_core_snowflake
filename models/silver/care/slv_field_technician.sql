{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'care'],
    transient=true
) }}
select
    technician_id,
    full_name                                               as technician_name,
    region_id,
    skills,
    coalesce(array_size(skills), 0)                         as skill_count,
    hired_on,
    home_base,
    coalesce(is_contractor, false)                          as is_contractor
from {{ source('care', 'field_technician') }}
