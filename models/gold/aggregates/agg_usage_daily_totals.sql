{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['gold', 'aggregates', 'usage', 'revenue', 'reconciliation', 'e19']
) }}
-- Network-wide usage and revenue per day x home region, reconciled against the legacy
-- pre-aggregated USAGE.USAGE_DAILY_AGG feed (which x_e19 read directly).
-- Ported from x_e19_usage_daily_totals (PIPE_X.SP_X_E19_USAGE_DAILY_TOTALS).
-- Grain   : event_date x region_name
-- Sources : fct_subscriber_usage_daily, fct_revenue_daily, int_subscriber_profile, usage.usage_daily_agg (bronze, legacy)
with prof as (
    select subscription_id, home_region_name as region_name, is_reportable
    from {{ ref('int_subscriber_profile') }}
),
u as (
    select coalesce(p.region_name, 'UNKNOWN') as region_name, f.event_date,
           count(*)                                         as active_lines,
           count_if(f.data_mb > 0)                          as data_active_lines,
           sum(f.data_mb)                                   as data_mb,
           sum(f.voice_min)                                 as voice_min,
           sum(f.sms_count)                                 as sms,
           sum(f.data_sessions)                             as data_sessions,
           sum(f.data_mb_5g)                                as data_mb_5g,
           count_if(not coalesce(p.is_reportable, true))    as non_reportable_lines
    from {{ ref('fct_subscriber_usage_daily') }} f
    left join prof p on p.subscription_id = f.subscription_id
    where f.event_date >= {{ f1_lower_bound('event_date', 7) }}
    group by 1, 2
),
r as (
    select coalesce(p.region_name, 'UNKNOWN') as region_name, rv.business_date as event_date,
           sum(rv.total_revenue_php)                        as revenue_php,
           sum(rv.recharge_revenue_php)                     as recharge_revenue_php
    from {{ ref('fct_revenue_daily') }} rv
    left join prof p on p.subscription_id = rv.subscription_id
    where rv.business_date >= {{ f1_lower_bound('event_date', 7) }}
    group by 1, 2
),
legacy as (
    select coalesce(p.region_name, 'UNKNOWN') as region_name, l.usage_date as event_date,
           count(*)                                         as legacy_active_lines,
           sum(l.data_mb)                                   as legacy_data_mb,
           sum(l.voice_min)                                 as legacy_voice_min,
           sum(l.sms_count)                                 as legacy_sms
    from {{ source('usage', 'usage_daily_agg') }} l
    left join prof p on p.subscription_id = l.subscription_id
    where l.usage_date >= {{ f1_lower_bound('event_date', 7) }}
    group by 1, 2
),
joined as (
    select
        coalesce(u.event_date, r.event_date, lg.event_date)        as event_date,
        coalesce(u.region_name, r.region_name, lg.region_name)     as region_name,
        zeroifnull(u.active_lines)                          as active_lines,
        zeroifnull(u.data_active_lines)                     as data_active_lines,
        round(zeroifnull(u.data_mb) / 1024, 3)              as data_gb,
        round(zeroifnull(u.data_mb_5g) / 1024, 3)           as data_gb_5g,
        round(zeroifnull(u.voice_min), 1)                   as voice_min,
        zeroifnull(u.sms)                                   as sms,
        zeroifnull(u.data_sessions)                         as data_sessions,
        zeroifnull(u.non_reportable_lines)                  as non_reportable_lines,
        round(zeroifnull(r.revenue_php), 2)                 as revenue_php,
        round(zeroifnull(r.recharge_revenue_php), 2)        as recharge_revenue_php,
        zeroifnull(lg.legacy_active_lines)                  as legacy_active_lines,
        round(zeroifnull(lg.legacy_data_mb) / 1024, 3)      as legacy_data_gb,
        round(zeroifnull(lg.legacy_voice_min), 1)           as legacy_voice_min,
        zeroifnull(lg.legacy_sms)                           as legacy_sms
    from u
    full outer join r  on r.event_date = u.event_date and r.region_name = u.region_name
    full outer join legacy lg
           on lg.event_date = coalesce(u.event_date, r.event_date)
          and lg.region_name = coalesce(u.region_name, r.region_name)
)
select
    j.*,
    round(div0(j.revenue_php, j.active_lines), 2)           as revenue_per_active_line_php,
    round(div0(j.data_gb_5g, j.data_gb) * 100, 2)           as data_5g_share_pct,
    round(div0(j.data_gb - j.legacy_data_gb, j.legacy_data_gb) * 100, 3)    as data_vs_legacy_pct,
    round(div0(j.voice_min - j.legacy_voice_min, j.legacy_voice_min) * 100, 3) as voice_vs_legacy_pct,
    round(div0(j.sms - j.legacy_sms, j.legacy_sms) * 100, 3) as sms_vs_legacy_pct,
    round(avg(j.revenue_php) over (partition by j.region_name order by j.event_date rows between 6 preceding and current row), 2) as revenue_php_7d_avg,
    round(ratio_to_report(j.revenue_php) over (partition by j.event_date), 6) as region_share_of_day_revenue,
    current_timestamp()                                     as loaded_at
from joined j
qualify j.event_date >= {{ f1_lower_bound('event_date') }}
    and j.event_date <= (select max(business_date) from {{ ref('fct_revenue_daily') }})
