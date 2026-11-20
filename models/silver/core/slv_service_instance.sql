{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'core'],
    transient=true
) }}
-- Fixed-broadband service instances (FTTH/DSL) with their validated service address.
select
    si.service_instance_id,
    si.subscription_id,
    si.service_type,
    si.ont_serial,
    si.ftth_node_id,
    {{ local_ntz_to_utc('si.provisioned_at') }}             as provisioned_at_utc,
    {{ local_ntz_to_utc('si.decommissioned_at') }}          as decommissioned_at_utc,
    coalesce(si.is_active, false)                           as is_active,
    sa.address_line,
    sa.barangay,
    sa.city,
    sa.zip_code,
    sa.location                                             as service_location,
    st_y(sa.location)                                       as latitude,
    st_x(sa.location)                                       as longitude,
    sa.validated_on
from {{ source('core', 'service_instance') }} si
left join {{ source('core', 'service_address') }} sa
       on sa.service_instance_id = si.service_instance_id
