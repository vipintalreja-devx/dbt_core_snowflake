{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'partner'],
    transient=true
) }}
select
    partner_brand_id,
    brand_name,
    upper(operator_type)                                    as operator_type,
    contract_start,
    revenue_share_pct::number(6, 2)                         as revenue_share_pct,
    coalesce(is_active, false)                              as is_active
from {{ source('partner', 'partner_brand') }}
