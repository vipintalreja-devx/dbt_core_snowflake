{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'network'],
    transient=true
) }}
select
    element_id,
    upper(element_type)                                     as element_type,
    vendor,
    model,
    cell_id,
    ip_address,
    firmware_version,
    {{ tz_to_utc('installed_at') }}                         as installed_at_utc
from {{ source('network', 'network_element') }}
