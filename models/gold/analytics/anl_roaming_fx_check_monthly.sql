{{ config(
    materialized='table',
    tags=['gold', 'analytics', 'roaming', 'revenue_assurance', 'm14']
) }}
-- Roaming revenue-assurance check per month and visited country: USD charges re-rated at the FX
-- rate valid on the usage day (ASOF JOIN) vs. the PHP amount actually billed.
-- Ported from x_m14_roaming_fx_check_monthly (PIPE_X.SP_X_M14_ROAMING_FX_CHECK_MONTHLY).
-- Grain: usage_month x visited_country.
-- Sources: source usage.roaming_usage (raw TAP records), slv_fx_rate, int_subscriber_profile, slv_roaming_usage.
with tap as (
    select subscription_id, usage_date, upper(visited_country) as visited_country, partner_operator,
           charge_usd, charge_php, data_mb, voice_min, sms_count
    from {{ source('usage', 'roaming_usage') }}
    where usage_date >= '{{ var("history_start_date") }}'::date
),
fx as (
    select valid_from as rate_date, rate
    from {{ ref('slv_fx_rate') }}
    where from_currency = 'USD' and to_currency = 'PHP'
),
rated as (
    select t.*, fx.rate, fx.rate_date,
           datediff('day', fx.rate_date, t.usage_date)      as rate_age_days
    from tap t
    asof join fx match_condition (t.usage_date >= fx.rate_date)
),
by_country as (
    select
        date_trunc('month', r.usage_date)::date             as usage_month,
        r.visited_country,
        count(*)                                            as tap_records,
        count(distinct r.subscription_id)                   as roamers,
        count(distinct r.partner_operator)                  as partner_operators,
        max_by(r.partner_operator, r.charge_usd)            as top_partner_by_charge,
        sum(r.data_mb)                                      as data_mb,
        sum(r.voice_min)                                    as voice_min,
        sum(r.sms_count)                                    as sms,
        sum(r.charge_usd)                                   as charge_usd,
        sum(r.charge_php)                                   as billed_php,
        sum(r.charge_usd * r.rate)                          as rerated_php,
        count_if(r.rate is null)                            as records_without_rate,
        max(r.rate_age_days)                                as max_rate_age_days,
        count(distinct p.home_region_name)                  as home_regions,
        count_if(p.plan_category = 'POSTPAID')              as postpaid_records
    from rated r
    left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = r.subscription_id
    group by 1, 2
),
silver_check as (
    select date_trunc('month', event_date)::date as usage_month, visited_country, sum(fx_variance_php) as silver_variance_php
    from {{ ref('slv_roaming_usage') }}
    group by 1, 2
)
select
    b.*,
    round(b.billed_php - b.rerated_php, 2)                  as variance_php,
    round(div0(b.billed_php - b.rerated_php, b.rerated_php) * 100, 3) as variance_pct,
    round(s.silver_variance_php, 2)                         as silver_variance_php,
    abs(div0(b.billed_php - b.rerated_php, b.rerated_php)) > 0.01 as is_material_variance,
    round(ratio_to_report(b.charge_usd) over (partition by b.usage_month), 4) as share_of_month_usd,
    current_timestamp()                                     as loaded_at
from by_country b
left join silver_check s on s.usage_month = b.usage_month and s.visited_country = b.visited_country
