{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'network'],
    transient=true
) }}
select
    c.cell_id,
    c.site_name,
    c.region_id,
    r.region_name,
    r.island_group,
    c.latitude,
    c.longitude,
    c.location                                              as cell_location,
    upper(c.radio_type)                                     as radio_type,
    {{ tz_to_utc('c.commissioned_at') }}                    as commissioned_at_utc
from {{ source('network', 'cell_site') }} c
left join {{ ref('slv_region') }} r on r.region_id = c.region_id
