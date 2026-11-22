{{ config(
    materialized='incremental',
    schema='silver',
    incremental_strategy='delete+insert',
    unique_key='churn_date',
    on_schema_change='append_new_columns',
    cluster_by=['churn_date'],
    tags=['insights', 'p02', 'churn']
) }}
-- P02 Churn root cause, step 1: every move into CHURNED, with the line's context at that moment.
-- Grain  : subscription_id x churn_date (a line can churn, win back and churn again)
-- Sources: slv_subscription_status_history (status intervals), int_subscriber_profile (region, plan),
--          fct_subscriber_monthly (average revenue of the 3 months before churn)
with moves as (
    select subscription_id, valid_from_date as churn_date, status,
           lag(status) over (partition by subscription_id order by valid_from_local, status_seq) as prev_status
    from {{ ref('slv_subscription_status_history') }}
    where valid_to_local is null or valid_to_local > valid_from_local
    qualify status = 'CHURNED' and coalesce(prev_status, '') <> 'CHURNED'
),
rev as (
    select m.subscription_id, m.churn_date,
           round(avg(f.total_revenue_php), 2)                          as avg_revenue_3m_php,
           max(f.tickets)                                              as max_tickets_month
    from moves m
    join {{ ref('fct_subscriber_monthly') }} f
      on f.subscription_id = m.subscription_id
     and f.month_start >= dateadd('month', -3, date_trunc('month', m.churn_date))
     and f.month_start <  date_trunc('month', m.churn_date)
    group by 1, 2
)
select
    m.subscription_id,
    m.churn_date,
    m.prev_status,
    p.home_region_id                                                    as region_id,
    p.home_region_name                                                  as region_name,
    p.plan_id, p.contract_type, p.plan_category, p.tenure_months, p.device_supports_5g,
    p.account_id,
    coalesce(r.avg_revenue_3m_php, 0)                                   as avg_revenue_3m_php
from moves m
left join rev r on r.subscription_id = m.subscription_id and r.churn_date = m.churn_date
left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = m.subscription_id
where m.churn_date >= {{ f1_lower_bound('churn_date') }}
