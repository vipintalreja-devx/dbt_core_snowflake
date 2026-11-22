{{ config(
    materialized='table',
    schema='gold',
    transient=false,
    copy_grants=true,
    tags=['insights', 'p03', 'ltv']
) }}
-- P03 Lifetime value by segment (contract type x plan category x island group).
-- Materialization: table (current estimate).
-- Grain  : ltv_segment
-- Sources: int_p03_cohort_members, fct_subscriber_monthly (last 6 closed months)
-- LTV    : monthly margin m, monthly retention r = 1 - hazard, annual discount 10% (d = 0.10/12):
--          LTV = m * r / (1 + d - r)  (geometric series of discounted surviving revenue).
--          Margin assumes 35% contribution margin on revenue (var insights_margin_pct).
with last6 as (
    select f.subscription_id, f.month_start, f.total_revenue_php, f.is_in_base_bom, f.is_churned
    from {{ ref('fct_subscriber_monthly') }} f
    where f.month_start >= dateadd('month', -6, (select max(month_start) from {{ ref('fct_subscriber_monthly') }}))
      and f.month_start <  (select max(month_start) from {{ ref('fct_subscriber_monthly') }})
),
seg as (
    select m.ltv_segment, m.contract_type, m.plan_category, m.island_group,
           count(distinct l.subscription_id)                             as lines,
           sum(iff(l.is_in_base_bom, 1, 0))                              as line_months_at_risk,
           sum(iff(l.is_churned, 1, 0))                                  as churns,
           avg(iff(l.is_in_base_bom, l.total_revenue_php, null))         as arpu_php
    from last6 l join {{ ref('int_p03_cohort_members') }} m on m.subscription_id = l.subscription_id
    group by 1, 2, 3, 4
)
select
    ltv_segment, contract_type, plan_category, island_group, lines,
    round(arpu_php, 2)                                                   as arpu_php,
    round(churns / nullif(line_months_at_risk, 0), 5)                    as monthly_churn_hazard,
    round(1 / nullif(churns / nullif(line_months_at_risk, 0), 0), 1)     as expected_lifetime_months,
    round(arpu_php * {{ var('insights_margin_pct', 0.35) }}
          * (1 - churns / nullif(line_months_at_risk, 0))
          / (1 + 0.10 / 12 - (1 - churns / nullif(line_months_at_risk, 0))), 2) as ltv_php,
    rank() over (order by arpu_php * (1 - churns / nullif(line_months_at_risk, 0))
                          / (1 + 0.10 / 12 - (1 - churns / nullif(line_months_at_risk, 0))) desc nulls last) as ltv_rank
from seg
where line_months_at_risk > 0
