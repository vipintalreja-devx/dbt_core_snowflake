{{ config(
    materialized='incremental',
    schema='gold',
    incremental_strategy='delete+insert',
    unique_key='activity_month',
    on_schema_change='append_new_columns',
    cluster_by=['activity_month'],
    tags=['insights', 'p03', 'cohort']
) }}
-- P03 Cohort activity per month: how many lines of each acquisition cohort are still in base and what
-- they earned.
-- Grain  : cohort_month x activity_month x contract_type x region_name
-- Sources: int_p03_cohort_members, fct_subscriber_monthly
select
    m.cohort_month,
    f.month_start                                                        as activity_month,
    datediff('month', m.cohort_month, f.month_start)                     as cohort_age_months,
    m.contract_type,
    m.region_id,
    m.region_name,
    count(*)                                                             as lines_observed,
    count_if(f.is_in_base_eom)                                           as lines_in_base_eom,
    count_if(f.is_churned)                                               as lines_churned,
    count_if(f.is_usage_active)                                          as lines_usage_active,
    round(sum(f.total_revenue_php), 2)                                   as revenue_php,
    round(sum(f.data_mb) / 1024, 2)                                      as data_gb
from {{ ref('fct_subscriber_monthly') }} f
join {{ ref('int_p03_cohort_members') }} m on m.subscription_id = f.subscription_id
where {{ f2_month_window('f.month_start', 'activity_month') }}
  and f.month_start >= m.cohort_month
group by 1, 2, 3, 4, 5, 6
