{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'partner'],
    transient=true,
    cluster_by=['partner_brand_id', 'effective_from']
) }}
-- Wholesale rate card with validity intervals (480 exact-duplicate versions removed).
select
    partner_brand_id,
    upper(service_type)                                     as service_type,
    unit,
    rate_php::number(18, 6)                                 as rate_php,
    effective_from,
    coalesce(effective_to, '9999-12-31'::date)              as effective_to,
    effective_to is null                                    as is_current
from {{ source('partner', 'partner_rate_card') }}
qualify row_number() over (partition by partner_brand_id, service_type, unit, effective_from
                           order by effective_to desc nulls first, rate_php desc) = 1
