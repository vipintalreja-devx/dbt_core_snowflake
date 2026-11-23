{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'core'],
    transient=true,
    post_hook=["{{ msisdn_mask_apply(['msisdn']) }}"]
) }}
-- SIM stock. AUTH_KEY (Ki) is a network secret and is deliberately NOT propagated.
select
    iccid,
    imsi,
    {{ normalize_msisdn('msisdn') }}                        as msisdn,
    sim_type,
    upper(status)                                           as sim_status,
    received_on,
    {{ tz_to_utc('activated_at') }}                         as activated_at_utc
from {{ source('core', 'sim_inventory') }}
