{{ config(
    materialized='incremental',
    schema='gold',
    tags=['insights', 'p04', 'roaming'],
    incremental_strategy='delete+insert',
    unique_key='event_date',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns'
) }}
-- P04 Roaming profitability: daily roaming margin.
-- Purpose : outbound roaming usage priced twice -- retail (what the subscriber was billed) and
--           wholesale IOT cost (what the visited network charges us): the reference tariff in
--           force that day (rate card + S3 partner files, the line's MVNO brand or the median
--           for own lines) x (1 - the visited operator's IOT discount, seed roaming_iot_discounts)
--           -- plus an FX check of the billed PHP against the USD charge at the BSP rate in force
--           (ASOF JOIN).
-- Grain   : event_date x visited_country x partner_operator x region_id
-- Sources : slv_roaming_usage, int_subscriber_profile, int_roaming_iot_rate_daily, slv_fx_rate, roaming_iot_discounts (seed)
with usage as (
    select r.event_date, r.subscription_id, r.visited_country, r.partner_operator,
           r.data_mb, r.voice_min, r.sms_count, r.charge_usd, r.charge_php_billed
    from {{ ref('slv_roaming_usage') }} r
    where r.event_date >= {{ f1_lower_bound('event_date') }}
),
lines as (
    select subscription_id, home_region_id, coalesce(partner_brand_id, 0) as partner_brand_id, is_mvno_line
    from {{ ref('int_subscriber_profile') }}
),
rates as (
    select rate_date, partner_brand_id,
           max(iff(service_group = 'ROAM_DATA',  rate_php, null)) as rate_data_mb,
           max(iff(service_group = 'ROAM_VOICE', rate_php, null)) as rate_voice_min,
           max(iff(service_group = 'IDD_SMS',    rate_php, null)) as rate_sms
    from {{ ref('int_roaming_iot_rate_daily') }}
    where rate_date >= {{ f1_lower_bound('event_date') }}
    group by 1, 2
),
fx as (
    select valid_from, rate as usd_php
    from {{ ref('slv_fx_rate') }}
    where from_currency = 'USD' and to_currency = 'PHP'
),
iot as (
    select partner_operator, visited_country, (1 - iot_discount_pct / 100) as iot_factor
    from {{ ref('roaming_iot_discounts') }}
),
priced as (
    select u.*,
           l.home_region_id,
           coalesce(l.is_mvno_line, false)                                  as is_mvno_line,
           coalesce(rb.rate_data_mb,   r0.rate_data_mb)   * d.iot_factor    as rate_data_mb,
           coalesce(rb.rate_voice_min, r0.rate_voice_min) * d.iot_factor    as rate_voice_min,
           coalesce(rb.rate_sms,       r0.rate_sms)       * d.iot_factor    as rate_sms
    from usage u
    left join iot d on d.partner_operator = u.partner_operator and d.visited_country = u.visited_country
    left join lines l on l.subscription_id = u.subscription_id
    left join rates rb on rb.rate_date = u.event_date and rb.partner_brand_id = l.partner_brand_id and l.partner_brand_id <> 0
    left join rates r0 on r0.rate_date = u.event_date and r0.partner_brand_id = 0
),
priced_fx as (
    select p.*, f.usd_php
    from priced p
    asof join fx f match_condition (p.event_date >= f.valid_from)
)
select
    event_date,
    visited_country,
    partner_operator,
    home_region_id                                                          as region_id,
    count(distinct subscription_id)                                         as roaming_lines,
    count_if(is_mvno_line)                                                  as mvno_line_days,
    sum(data_mb)                                                            as data_mb,
    sum(voice_min)                                                          as voice_min,
    sum(sms_count)                                                          as sms_count,
    round(sum(charge_php_billed), 2)                                        as retail_revenue_php,
    round(sum(charge_usd * usd_php), 2)                                     as retail_at_bsp_fx_php,
    round(sum(charge_php_billed) - sum(charge_usd * usd_php), 2)            as fx_variance_php,
    round(sum(data_mb * rate_data_mb), 2)                                   as wholesale_data_php,
    round(sum(voice_min * rate_voice_min), 2)                               as wholesale_voice_php,
    round(sum(sms_count * rate_sms), 2)                                     as wholesale_sms_php,
    round(sum(data_mb * rate_data_mb) + sum(voice_min * rate_voice_min) + sum(sms_count * rate_sms), 2) as wholesale_cost_php,
    round(sum(charge_php_billed)
          - (sum(data_mb * rate_data_mb) + sum(voice_min * rate_voice_min) + sum(sms_count * rate_sms)), 2) as margin_php,
    round(div0(sum(charge_php_billed)
          - (sum(data_mb * rate_data_mb) + sum(voice_min * rate_voice_min) + sum(sms_count * rate_sms)),
               sum(charge_php_billed)) * 100, 2)                            as margin_pct,
    count_if(rate_data_mb is null or rate_voice_min is null or rate_sms is null) as unpriced_records,
    {{ dbt_loaded_at() }}                                                   as _dbt_loaded_at
from priced_fx
group by 1, 2, 3, 4
