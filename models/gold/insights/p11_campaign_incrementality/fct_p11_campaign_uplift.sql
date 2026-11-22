{{ config(
    materialized='incremental',
    schema='gold',
    tags=['insights', 'p11'],
    incremental_strategy='delete+insert',
    unique_key='send_month',
    on_schema_change='append_new_columns',
    cluster_by=['send_month'],
    transient=false
) }}
-- P11 campaign incrementality | output 1: true revenue uplift of each campaign per send month.
-- Grain   : campaign_id x send_month
-- Method  : stratified difference-in-differences. For every treated line, revenue in the 30 days
--           after its first send minus the 30 days before; the expected change is the same-window
--           change per line of the untargeted pool in the same stratum (region x plan x churn band
--           x tenure). Uplift = treated change - control change; t-stat on the per-line uplift.
--           Changes are compared as daily revenue rates (pre: 30 days; post: the days loaded so far,
--           at most 30) scaled to 30 days, so a post window cut off by the latest load date does
--           not bias the comparison; lines with fewer than 7 post days get no uplift yet.
-- Sources : int_p11_campaign_treated, int_p11_control_stratum_daily, fct_revenue_daily, slv_campaign
{% set lb = f3_month_lower_bound('send_month') %}
{% set rev_max = p_max_date_literal(ref('fct_revenue_daily'), 'business_date') %}
with treated as (
    select * from {{ ref('int_p11_campaign_treated') }} where send_month >= {{ lb }}
),
t_rev as (
    select t.campaign_id, t.subscription_id, t.send_month, t.first_send_date, t.stratum_key,
           t.is_opened, t.is_converted,
           coalesce(sum(iff(r.business_date <  t.first_send_date, r.total_revenue_php, 0)), 0) as pre_rev_php,
           coalesce(sum(iff(r.business_date >= t.first_send_date, r.total_revenue_php, 0)), 0) as post_rev_php,
           least(30, datediff('day', t.first_send_date, {{ rev_max }}) + 1)                      as post_days
    from treated t
    left join {{ ref('fct_revenue_daily') }} r
           on r.subscription_id = t.subscription_id
          and r.business_date between dateadd('day', -30, t.first_send_date) and dateadd('day', 29, t.first_send_date)
          and r.business_date >= dateadd('day', -31, {{ lb }})
    group by 1, 2, 3, 4, 5, 6, 7
),
anchors as (
    select distinct send_month, stratum_key, first_send_date,
           least(30, datediff('day', first_send_date, {{ rev_max }}) + 1) as post_days
    from treated
),
ctrl as (
    select a.send_month, a.stratum_key, a.first_send_date,
           sum(iff(c.business_date <  a.first_send_date, c.revenue_php, 0)) / max(c.pool_lines) / 30 as ctrl_pre_rate,
           sum(iff(c.business_date >= a.first_send_date, c.revenue_php, 0)) / max(c.pool_lines) / nullif(greatest(max(a.post_days), 0), 0) as ctrl_post_rate,
           max(c.pool_lines)                                as ctrl_pool_lines
    from anchors a
    join {{ ref('int_p11_control_stratum_daily') }} c
      on c.send_month = a.send_month and c.stratum_key = a.stratum_key
     and c.business_date between dateadd('day', -30, a.first_send_date) and dateadd('day', 29, a.first_send_date)
    where c.send_month >= {{ lb }}
    group by 1, 2, 3
),
lines as (
    select t.*,
           iff(t.post_days >= 7, (t.post_rev_php / nullif(greatest(t.post_days, 0), 0) - t.pre_rev_php / 30) * 30, null)       as treated_change_php,
           iff(t.post_days >= 7, (c.ctrl_post_rate - c.ctrl_pre_rate) * 30, null)                     as control_change_php,
           iff(t.post_days >= 7, (t.post_rev_php / nullif(greatest(t.post_days, 0), 0) - t.pre_rev_php / 30) * 30
                                 - (c.ctrl_post_rate - c.ctrl_pre_rate) * 30, null)                     as uplift_php,
           c.ctrl_pool_lines
    from t_rev t
    left join ctrl c on c.send_month = t.send_month and c.stratum_key = t.stratum_key and c.first_send_date = t.first_send_date
),
campaign_sends as (
    select campaign_id, count(*) as treated_rows_all_months
    from {{ ref('int_p11_campaign_treated') }}
    group by 1
),
agg as (
    select
        l.campaign_id, l.send_month,
        count(*)                                            as treated_lines,
        count(l.uplift_php)                                 as matched_lines,
        round(avg(iff(l.is_opened, 1, 0)), 4)               as open_rate,
        round(avg(iff(l.is_converted, 1, 0)), 4)            as conversion_rate,
        round(avg(l.pre_rev_php), 2)                        as treated_pre_rev_per_line,
        round(avg(l.post_rev_php), 2)                       as treated_post_rev_per_line,
        min(l.post_days)                                    as min_post_days,
        round(avg(l.treated_change_php), 2)                 as treated_change_per_line,
        round(avg(l.control_change_php), 2)                 as control_change_per_line,
        round(avg(l.uplift_php), 2)                         as uplift_per_line_php,
        round(stddev_samp(l.uplift_php), 2)                 as uplift_stddev_php,
        round(avg(iff(l.is_converted, l.uplift_php, null)), 2)     as uplift_per_converter_php,
        round(avg(iff(not l.is_converted, l.uplift_php, null)), 2) as uplift_per_non_converter_php,
        min(l.first_send_date)                              as first_send_date,
        max(l.first_send_date)                              as last_send_date,
        count(distinct l.stratum_key)                       as strata
    from lines l
    group by 1, 2
)
select
    a.campaign_id, a.send_month,
    c.campaign_name, c.channel, c.start_date, c.end_date, c.budget_php,
    a.treated_lines, a.matched_lines, round(a.matched_lines / a.treated_lines, 4) as matched_share,
    a.open_rate, a.conversion_rate,
    a.treated_pre_rev_per_line, a.treated_post_rev_per_line, a.min_post_days, a.treated_change_per_line,
    a.control_change_per_line, a.uplift_per_line_php, a.uplift_stddev_php,
    round(a.uplift_per_line_php / nullif(a.uplift_stddev_php / sqrt(nullif(a.matched_lines, 0)), 0), 2) as uplift_t_stat,
    abs(a.uplift_per_line_php / nullif(a.uplift_stddev_php / sqrt(nullif(a.matched_lines, 0)), 0)) >= 1.96 as is_significant,
    a.uplift_per_converter_php, a.uplift_per_non_converter_php,
    round(a.uplift_per_line_php * a.matched_lines, 2)      as incremental_revenue_php,
    round(c.budget_php * a.treated_lines / nullif(s.treated_rows_all_months, 0), 2) as budget_allocated_php,
    round((a.uplift_per_line_php * a.matched_lines) / nullif(c.budget_php * a.treated_lines / nullif(s.treated_rows_all_months, 0), 0), 3) as incremental_roi,
    a.strata, a.first_send_date, a.last_send_date,
    dateadd('day', 29, a.last_send_date) <= {{ rev_max }}   as is_post_window_complete,
    current_timestamp()                                     as loaded_at
from agg a
left join {{ ref('slv_campaign') }} c on c.campaign_id = a.campaign_id
left join campaign_sends s on s.campaign_id = a.campaign_id
