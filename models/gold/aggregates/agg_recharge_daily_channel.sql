{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['gold', 'aggregates', 'billing', 'prepaid', 'e01']
) }}
-- Prepaid top-ups per Manila business day x channel x pack x home region.
-- Ported from x_e01_recharge_daily_channel (PIPE_X.SP_X_E01_RECHARGE_DAILY_CHANNEL); the procedure
-- computed one day per CALL - this model maintains every day of the window incrementally.
-- Grain   : event_date x recharge_channel x pack_code x home_region_name
-- Sources : slv_recharge, int_subscriber_profile, dim_region, dim_date
with recharge as (
    select r.recharge_id, r.subscription_id, r.event_date, r.recharged_at_local,
           r.pack_code, r.recharge_channel, r.recharge_amount_php
    from {{ ref('slv_recharge') }} r
    where r.event_date >= {{ f1_lower_bound('event_date', 7) }}
),
enriched as (
    select rc.*,
           coalesce(p.home_region_name, 'UNKNOWN')          as home_region_name,
           coalesce(rg.island_group, 'UNKNOWN')             as island_group,
           coalesce(p.plan_category, 'UNKNOWN')             as plan_category,
           coalesce(p.is_reportable, true)                  as is_reportable
    from recharge rc
    left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = rc.subscription_id
    left join {{ ref('dim_region') }} rg             on rg.region_id = p.home_region_id
),
hourly as (
    -- busy hour per group: the hour with the most top-ups
    select event_date, recharge_channel, pack_code, home_region_name,
           hour(recharged_at_local) as recharge_hour, count(*) as hour_recharges
    from enriched
    group by 1, 2, 3, 4, 5
),
busy as (
    select event_date, recharge_channel, pack_code, home_region_name,
           max_by(recharge_hour, hour_recharges)            as busy_hour
    from hourly
    group by 1, 2, 3, 4
),
daily as (
    select
        e.event_date,
        e.recharge_channel,
        e.pack_code,
        e.home_region_name,
        any_value(e.island_group)                           as island_group,
        count(*)                                            as recharges,
        approx_count_distinct(e.subscription_id)            as subscribers_approx,
        sum(e.recharge_amount_php)                          as amount_php,
        round(avg(e.recharge_amount_php), 2)                as avg_amount_php,
        median(e.recharge_amount_php)                       as median_amount_php,
        count_if(e.plan_category = 'PREPAID_FLEX')          as recharges_prepaid_flex,
        count_if(not e.is_reportable)                       as recharges_non_reportable
    from enriched e
    group by 1, 2, 3, 4
),
scored as (
    select
        d.*,
        b.busy_hour,
        dd.day_name,
        dd.is_weekend,
        round(ratio_to_report(d.amount_php) over (partition by d.event_date), 6)               as share_of_day_amount,
        round(ratio_to_report(d.amount_php) over (partition by d.event_date, d.recharge_channel), 6) as share_of_channel_amount,
        lag(d.amount_php) over (partition by d.recharge_channel, d.pack_code, d.home_region_name order by d.event_date) as prev_day_amount_php,
        round(avg(d.amount_php) over (partition by d.recharge_channel, d.pack_code, d.home_region_name
                                      order by d.event_date rows between 6 preceding and current row), 2) as amount_php_7d_avg
    from daily d
    left join busy b
           on b.event_date = d.event_date and b.recharge_channel = d.recharge_channel
          and b.pack_code = d.pack_code and b.home_region_name = d.home_region_name
    left join {{ ref('dim_date') }} dd on dd.date_day = d.event_date
)
select
    *,
    round(div0(amount_php - prev_day_amount_php, prev_day_amount_php) * 100, 2) as amount_dod_pct,
    current_timestamp()                                     as loaded_at
from scored
where event_date >= {{ f1_lower_bound('event_date') }}
