{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='revenue_month',
    on_schema_change='append_new_columns',
    tags=['gold', 'analytics', 'partner', 'wholesale', 'm13']
) }}
-- MVNO partner economics per month: revenue by stream from partner lines, contractual share vs.
-- booked settlement, and the wholesale value of partner roaming/IDD traffic at the rate card in force.
-- Ported from x_m13_partner_revenue_monthly (PIPE_X.SP_X_M13_PARTNER_REVENUE_MONTHLY).
-- Grain: partner_brand_id x revenue_month.
-- Sources: fct_subscriber_monthly, slv_partner_subscriber_map, slv_partner_brand, slv_partner_settlement,
--          slv_partner_rate_card, slv_roaming_usage.
with lines as (
    select
        pm.partner_brand_id,
        m.month_start                                       as revenue_month,
        count(distinct m.subscription_id)                   as lines_with_activity,
        count_if(m.is_in_base_eom)                          as lines_in_base_eom,
        count(distinct pm.wholesale_plan)                   as wholesale_plans,
        sum(m.plan_revenue_php)                             as plan_revenue_php,
        sum(m.recharge_revenue_php)                         as recharge_revenue_php,
        sum(m.vas_revenue_php)                              as vas_revenue_php,
        sum(m.roaming_revenue_php)                          as roaming_revenue_php,
        sum(m.total_revenue_php)                            as gross_revenue_php,
        sum(m.data_mb) / 1024                               as data_gb,
        sum(m.voice_min)                                    as voice_min
    from {{ ref('fct_subscriber_monthly') }} m
    join {{ ref('slv_partner_subscriber_map') }} pm on pm.subscription_id = m.subscription_id
    where {{ f2_month_window('m.month_start', 'revenue_month') }}
    group by 1, 2
),
-- rate card in force at month end, averaged per service family (service_type carries a random suffix)
rates as (
    select
        l.partner_brand_id,
        l.revenue_month,
        avg(iff(regexp_substr(rc.service_type, '^[A-Z]+_[A-Z]+') = 'ROAM_DATA',  rc.rate_php, null)) as roam_data_rate_per_mb,
        avg(iff(regexp_substr(rc.service_type, '^[A-Z]+_[A-Z]+') = 'ROAM_VOICE', rc.rate_php, null)) as roam_voice_rate_per_min
    from lines l
    join {{ ref('slv_partner_rate_card') }} rc
      on rc.partner_brand_id = l.partner_brand_id
     and last_day(l.revenue_month) >= rc.effective_from
     and last_day(l.revenue_month) <  rc.effective_to
    group by 1, 2
),
roam as (
    select
        pm.partner_brand_id,
        date_trunc('month', r.event_date)::date             as revenue_month,
        sum(r.data_mb)                                      as roaming_mb,
        sum(r.voice_min)                                    as roaming_min
    from {{ ref('slv_roaming_usage') }} r
    join {{ ref('slv_partner_subscriber_map') }} pm on pm.subscription_id = r.subscription_id
    group by 1, 2
)
select
    l.partner_brand_id,
    l.revenue_month,
    b.brand_name,
    b.operator_type,
    b.revenue_share_pct,
    l.lines_with_activity,
    l.lines_in_base_eom,
    l.wholesale_plans,
    round(l.plan_revenue_php, 2)                            as plan_revenue_php,
    round(l.recharge_revenue_php, 2)                        as recharge_revenue_php,
    round(l.vas_revenue_php, 2)                             as vas_revenue_php,
    round(l.roaming_revenue_php, 2)                         as roaming_revenue_php,
    round(l.gross_revenue_php, 2)                           as gross_revenue_php,
    round(div0(l.gross_revenue_php, l.lines_in_base_eom), 2) as arpu_php,
    round(l.data_gb, 2)                                     as data_gb,
    round(l.voice_min, 1)                                   as voice_min,
    round(l.gross_revenue_php * b.revenue_share_pct / 100, 2) as expected_share_php,
    s.share_php                                             as settled_share_php,
    s.settlement_status,
    round(zeroifnull(s.share_php) - l.gross_revenue_php * b.revenue_share_pct / 100, 2) as share_variance_php,
    round(zeroifnull(ro.roaming_mb), 2)                     as roaming_mb,
    round(zeroifnull(ro.roaming_min), 2)                    as roaming_min,
    round(zeroifnull(ro.roaming_mb) * r.roam_data_rate_per_mb
          + zeroifnull(ro.roaming_min) * r.roam_voice_rate_per_min, 2) as roaming_wholesale_value_php,
    round(div0(l.gross_revenue_php - lag(l.gross_revenue_php) over (partition by l.partner_brand_id order by l.revenue_month),
               lag(l.gross_revenue_php) over (partition by l.partner_brand_id order by l.revenue_month)) * 100, 2) as revenue_mom_pct,
    current_timestamp()                                     as loaded_at
from lines l
join {{ ref('slv_partner_brand') }} b on b.partner_brand_id = l.partner_brand_id
left join {{ ref('slv_partner_settlement') }} s
       on s.partner_brand_id = l.partner_brand_id and s.settlement_month = l.revenue_month
left join rates r on r.partner_brand_id = l.partner_brand_id and r.revenue_month = l.revenue_month
left join roam ro on ro.partner_brand_id = l.partner_brand_id and ro.revenue_month = l.revenue_month
