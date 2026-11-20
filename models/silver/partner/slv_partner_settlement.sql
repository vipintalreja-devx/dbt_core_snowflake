{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'partner'],
    transient=true
) }}
select
    settlement_id,
    partner_brand_id,
    settlement_month,
    gross_revenue_php::number(18, 2)                        as gross_revenue_php,
    share_php::number(18, 2)                                as share_php,
    fx_rate,
    {{ tz_to_utc('settled_at') }}                           as settled_at_utc,
    upper(status)                                           as settlement_status
from {{ source('partner', 'partner_settlement') }}
