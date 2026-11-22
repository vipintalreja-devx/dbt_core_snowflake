{{ config(
    materialized='incremental',
    schema='silver',
    tags=['insights', 'p08'],
    incremental_strategy='delete+insert',
    unique_key='month_start',
    on_schema_change='append_new_columns',
    cluster_by=['month_start'],
    transient=true
) }}
-- P08 cell site ROI | step 2: recognised revenue of the lines homed on each cell, per month.
-- Grain   : home_cell_id x month_start
-- Sources : slv_daily_revenue (revenue streams), int_subscriber_profile (home cell, reportability)
with lines as (
    select subscription_id, home_cell_id, plan_category
    from {{ ref('int_subscriber_profile') }}
    where home_cell_id is not null and not is_test
),
rev as (
    select r.subscription_id,
           date_trunc('month', r.business_date)::date       as month_start,
           sum(r.revenue_php)                               as revenue_php,
           sum(iff(r.revenue_source = 'PLAN', r.revenue_php, 0))     as plan_revenue_php,
           sum(iff(r.revenue_source = 'RECHARGE', r.revenue_php, 0)) as recharge_revenue_php,
           sum(iff(r.revenue_source = 'VAS', r.revenue_php, 0))      as vas_revenue_php,
           sum(iff(r.revenue_source = 'ROAMING', r.revenue_php, 0))  as roaming_revenue_php
    from {{ ref('slv_daily_revenue') }} r
    where {{ f2_month_window('r.business_date', 'month_start') }}
    group by 1, 2
)
select
    l.home_cell_id,
    r.month_start,
    count(distinct r.subscription_id)                       as revenue_lines,
    count(distinct iff(l.plan_category = 'POSTPAID', r.subscription_id, null)) as postpaid_lines,
    round(sum(r.revenue_php), 2)                            as revenue_php,
    round(sum(r.plan_revenue_php), 2)                       as plan_revenue_php,
    round(sum(r.recharge_revenue_php), 2)                   as recharge_revenue_php,
    round(sum(r.vas_revenue_php), 2)                        as vas_revenue_php,
    round(sum(r.roaming_revenue_php), 2)                    as roaming_revenue_php,
    round(sum(r.revenue_php) / nullif(count(distinct r.subscription_id), 0), 2) as arpu_php
from rev r
join lines l on l.subscription_id = r.subscription_id
group by 1, 2
