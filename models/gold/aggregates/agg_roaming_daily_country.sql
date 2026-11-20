{{ config(
    materialized='table',
    cluster_by=['event_date'],
    tags=['gold', 'aggregates', 'usage', 'roaming', 'e15']
) }}
-- Outbound roaming per day x visited country x partner operator, with FX check of the billed PHP.
-- Ported from x_e15_roaming_daily_country (PIPE_X.SP_X_E15_ROAMING_DAILY_COUNTRY).
-- Grain   : event_date x visited_country x partner_operator
-- Sources : slv_roaming_usage (FX as-of joined), int_subscriber_profile, dim_date
with ru as (
    select
        r.subscription_id, r.event_date, r.visited_country, r.partner_operator,
        r.data_mb, r.voice_min, r.sms_count, r.charge_usd, r.charge_php_billed, r.charge_php_at_fx,
        r.fx_variance_php, r.usd_php_rate,
        coalesce(p.plan_category, 'UNKNOWN')                as plan_category,
        coalesce(p.home_region_name, 'UNKNOWN')             as home_region_name
    from {{ ref('slv_roaming_usage') }} r
    left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = r.subscription_id
),
daily as (
    select
        event_date, visited_country, partner_operator,
        approx_count_distinct(subscription_id)              as roamers_approx,
        count(distinct subscription_id)                     as roamers,
        sum(data_mb)                                        as data_mb,
        sum(voice_min)                                      as voice_min,
        sum(sms_count)                                      as sms,
        sum(charge_usd)                                     as charge_usd,
        sum(charge_php_billed)                              as charge_php_billed,
        sum(charge_php_at_fx)                               as charge_php_at_fx,
        sum(fx_variance_php)                                as fx_variance_php,
        max(usd_php_rate)                                   as usd_php_rate,
        count_if(plan_category = 'POSTPAID')                as postpaid_roamer_days,
        mode(home_region_name)                              as top_home_region
    from ru
    group by 1, 2, 3
)
select
    d.*,
    round(div0(d.charge_php_billed, d.roamers), 2)          as charge_php_per_roamer,
    round(div0(d.data_mb, d.roamers), 1)                    as data_mb_per_roamer,
    round(div0(d.fx_variance_php, d.charge_php_at_fx) * 100, 3) as fx_variance_pct,
    round(ratio_to_report(d.charge_php_billed) over (partition by d.event_date), 6) as share_of_day_charge,
    round(avg(d.roamers) over (partition by d.visited_country, d.partner_operator order by d.event_date
                               rows between 6 preceding and current row), 1) as roamers_7d_avg,
    dense_rank() over (partition by d.event_date order by d.charge_php_billed desc) as country_rank_of_day,
    dd.is_weekend,
    current_timestamp()                                     as loaded_at
from daily d
left join {{ ref('dim_date') }} dd on dd.date_day = d.event_date
