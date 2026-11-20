{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'usage'],
    transient=true
) }}
-- TAP roaming usage with USD charges converted to PHP at the FX rate valid on the usage day.
with ru as (
    select * from {{ source('usage', 'roaming_usage') }}
    where usage_date >= '{{ var("history_start_date") }}'::date
),
fx as (
    select valid_from, valid_to, rate
    from {{ ref('slv_fx_rate') }}
    where from_currency = 'USD' and to_currency = 'PHP'
)
select
    ru.subscription_id,
    ru.usage_date                                           as event_date,
    upper(ru.visited_country)                               as visited_country,
    ru.partner_operator,
    zeroifnull(ru.data_mb)                                  as data_mb,
    zeroifnull(ru.voice_min)                                as voice_min,
    zeroifnull(ru.sms_count)                                as sms_count,
    ru.charge_usd::number(18, 4)                            as charge_usd,
    ru.charge_php::number(18, 4)                            as charge_php_billed,
    fx.rate                                                 as usd_php_rate,
    round(ru.charge_usd * fx.rate, 4)                       as charge_php_at_fx,
    round(ru.charge_php - ru.charge_usd * fx.rate, 4)       as fx_variance_php
from ru
left join fx
       on ru.usage_date >= fx.valid_from
      and ru.usage_date <  fx.valid_to
