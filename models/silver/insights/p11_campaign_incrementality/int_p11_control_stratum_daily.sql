{{ config(
    materialized='incremental',
    schema='silver',
    tags=['insights', 'p11'],
    incremental_strategy='delete+insert',
    unique_key='send_month',
    on_schema_change='append_new_columns',
    cluster_by=['send_month'],
    transient=true
) }}
-- P11 campaign incrementality | step 2: daily revenue of the control pool per stratum.
-- Control pool of a month = active, reportable lines that no campaign targeted in that month.
-- Grain   : send_month x stratum_key x business_date (business dates from 31 days before to 31 days
--           after the send month, so every send date has a full pre and post window)
-- Sources : int_p11_campaign_treated, int_subscriber_profile, slv_daily_revenue
{% set lb = f3_month_lower_bound('send_month') %}
with months as (
    select distinct send_month from {{ ref('int_p11_campaign_treated') }} where send_month >= {{ lb }}
),
targeted as (
    select distinct send_month, subscription_id from {{ ref('int_p11_campaign_treated') }} where send_month >= {{ lb }}
),
lines as (
    select subscription_id,
           concat_ws('|', coalesce(home_region_id, -1)::varchar, plan_category,
                     coalesce(churn_band, 'UNSCORED'), {{ p_tenure_band('tenure_months') }}) as stratum_key
    from {{ ref('int_subscriber_profile') }}
    where is_reportable and status = 'ACTIVE'
),
pool as (
    select m.send_month, l.subscription_id, l.stratum_key
    from months m
    cross join lines l
    left join targeted t on t.send_month = m.send_month and t.subscription_id = l.subscription_id
    where t.subscription_id is null
),
pool_size as (
    select send_month, stratum_key, count(*) as pool_lines from pool group by 1, 2
),
rev as (
    select subscription_id, business_date, sum(revenue_php) as revenue_php
    from {{ ref('slv_daily_revenue') }}
    where business_date >= dateadd('day', -31, {{ lb }})
    group by 1, 2
)
select
    p.send_month,
    p.stratum_key,
    r.business_date,
    any_value(s.pool_lines)                                 as pool_lines,
    count(distinct r.subscription_id)                       as lines_with_revenue,
    round(sum(r.revenue_php), 2)                            as revenue_php
from pool p
join rev r on r.subscription_id = p.subscription_id
          and r.business_date between dateadd('day', -31, p.send_month) and dateadd('day', 31, last_day(p.send_month))
join pool_size s on s.send_month = p.send_month and s.stratum_key = p.stratum_key
group by 1, 2, 3
