{{ config(
    materialized='incremental',
    schema='gold',
    tags=['insights', 'p05', 'partner'],
    incremental_strategy='delete+insert',
    unique_key='month_start',
    cluster_by=['month_start', 'partner_brand_id'],
    on_schema_change='append_new_columns'
) }}
-- P05 MVNO settlement: one row per MVNO line and month, with the wholesale charge the host
-- network bills the partner. Usage is unpivoted into one row per service, priced with the
-- partner's wholesale rate in force at the start of the month (ASOF JOIN on the rate card,
-- DATA / VOICE / SMS) and pivoted back.
-- Grain   : month_start x subscription_id
-- Sources : slv_partner_subscriber_map, slv_partner_brand, fct_subscriber_monthly, slv_partner_rate_card
with mvno_lines as (
    select m.subscription_id, m.partner_brand_id, m.wholesale_plan, m.onboarded_at_utc, b.brand_name
    from {{ ref('slv_partner_subscriber_map') }} m
    join {{ ref('slv_partner_brand') }} b on b.partner_brand_id = m.partner_brand_id
),
line_month as (
    select f.month_start, f.subscription_id, l.partner_brand_id, l.brand_name, l.wholesale_plan,
           f.region_name, f.status_eom, f.is_in_base_bom, f.is_in_base_eom, f.is_gross_add, f.is_churned,
           f.active_days, f.voice_min, f.sms_count, f.data_mb, f.total_revenue_php
    from {{ ref('fct_subscriber_monthly') }} f
    join mvno_lines l on l.subscription_id = f.subscription_id
    where {{ f2_month_window('f.month_start', 'month_start') }}
),
units as (
    select month_start, subscription_id, partner_brand_id, service_type, units
    from (select month_start, subscription_id, partner_brand_id,
                 voice_min::number(38, 4) as voice_min, sms_count::number(38, 4) as sms_count, data_mb::number(38, 4) as data_mb
          from line_month)
    unpivot (units for service_type in (voice_min, sms_count, data_mb))
),
wholesale_rates as (
    select partner_brand_id, service_type, effective_from, effective_to, rate_php
    from {{ ref('slv_partner_rate_card') }}
    where service_type in ('DATA', 'VOICE', 'SMS')
),
priced as (
    select u.month_start, u.subscription_id, u.service_type, u.units,
           iff(r.effective_to >= u.month_start, r.rate_php, null)           as rate_php
    from (select *, case service_type when 'VOICE_MIN' then 'VOICE' when 'SMS_COUNT' then 'SMS' else 'DATA' end as rate_service
          from units) u
    asof join wholesale_rates r
        match_condition (u.month_start >= r.effective_from)
        on u.partner_brand_id = r.partner_brand_id and u.rate_service = r.service_type
),
charges as (
    select *
    from (select month_start, subscription_id, service_type, round(units * rate_php, 4) as charge_php from priced)
    pivot (sum(charge_php) for service_type in ('VOICE_MIN', 'SMS_COUNT', 'DATA_MB'))
        as p (month_start, subscription_id, wholesale_voice_php, wholesale_sms_php, wholesale_data_php)
)
select
    lm.month_start, lm.subscription_id, lm.partner_brand_id, lm.brand_name, lm.wholesale_plan, lm.region_name,
    lm.status_eom, lm.is_in_base_bom, lm.is_in_base_eom, lm.is_gross_add, lm.is_churned, lm.active_days,
    lm.voice_min, lm.sms_count, lm.data_mb,
    round(lm.total_revenue_php, 2)                                          as retail_revenue_php,
    zeroifnull(c.wholesale_voice_php)                                       as wholesale_voice_php,
    zeroifnull(c.wholesale_sms_php)                                         as wholesale_sms_php,
    zeroifnull(c.wholesale_data_php)                                        as wholesale_data_php,
    round(zeroifnull(c.wholesale_voice_php) + zeroifnull(c.wholesale_sms_php) + zeroifnull(c.wholesale_data_php), 2) as wholesale_charge_php,
    {{ dbt_loaded_at() }}                                                   as _dbt_loaded_at
from line_month lm
left join charges c on c.month_start = lm.month_start and c.subscription_id = lm.subscription_id
