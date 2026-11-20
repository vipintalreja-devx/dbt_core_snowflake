{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'ref'],
    transient=true
) }}
select
    device_id,
    manufacturer,
    model,
    device_type,
    coalesce(supports_5g, false)                            as supports_5g,
    launch_date,
    list_price_php::number(18, 2)                           as list_price_php,
    specs
from {{ source('ref', 'device_catalog') }}
qualify row_number() over (partition by device_id order by launch_date desc nulls last) = 1
