{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='business_date',
    on_schema_change='append_new_columns',
    cluster_by=['business_date'],
    tags=['gold', 'analytics', 'revenue', 'm01']
) }}
-- Daily earned revenue per tariff plan and revenue stream, with base, ARPU-per-day and mix shares.
-- Ported from x_m01_revenue_daily_by_plan (PIPE_X.SP_X_M01_REVENUE_DAILY_BY_PLAN).
-- Grain: business_date x plan_id x revenue_source.
-- Sources: slv_daily_revenue, int_subscriber_profile, slv_plan, slv_subscription_status_history.
with rev as (
    select r.business_date, r.subscription_id, r.revenue_source, r.revenue_php
    from {{ ref('slv_daily_revenue') }} r
    where {{ incremental_date_filter('r.business_date', 'business_date') }}
),
-- billable base per plan on each day (status interval covering the day)
base as (
    select d.business_date, p.plan_id, count(distinct h.subscription_id) as billable_lines
    from (select distinct business_date from rev) d
    join {{ ref('slv_subscription_status_history') }} h
      on h.is_in_base
     and d.business_date >= h.valid_from_date
     and d.business_date <  h.valid_to_date
    join {{ ref('int_subscriber_profile') }} p
      on p.subscription_id = h.subscription_id
     and p.is_reportable
    group by 1, 2
),
agg as (
    select
        r.business_date,
        p.plan_id,
        r.revenue_source,
        count(distinct r.subscription_id)                   as revenue_lines,
        sum(r.revenue_php)                                  as revenue_php,
        count_if(r.revenue_php < 0)                         as negative_revenue_rows
    from rev r
    join {{ ref('int_subscriber_profile') }} p
      on p.subscription_id = r.subscription_id
     and p.is_reportable
    group by 1, 2, 3
)
select
    a.business_date,
    a.plan_id,
    pl.plan_name,
    pl.contract_type,
    pl.plan_category,
    pl.base_price_php,
    a.revenue_source,
    a.revenue_lines,
    zeroifnull(b.billable_lines)                            as billable_lines,
    round(a.revenue_php, 4)                                 as revenue_php,
    a.negative_revenue_rows,
    round(div0(a.revenue_php, b.billable_lines), 4)         as revenue_per_billable_line_php,
    round(ratio_to_report(a.revenue_php) over (partition by a.business_date, a.plan_id), 4) as stream_share_in_plan,
    round(ratio_to_report(a.revenue_php) over (partition by a.business_date, a.revenue_source), 6) as plan_share_in_stream,
    dense_rank() over (partition by a.business_date, a.revenue_source order by a.revenue_php desc) as plan_rank_in_stream,
    current_timestamp()                                     as loaded_at
from agg a
left join {{ ref('slv_plan') }} pl on pl.plan_id = a.plan_id
left join base b on b.business_date = a.business_date and b.plan_id = a.plan_id
