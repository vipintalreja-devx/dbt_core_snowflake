{{ config(
    materialized='table',
    schema='gold',
    transient=false,
    copy_grants=true,
    tags=['insights', 'p03', 'cohort']
) }}
-- P03 Cohort retention curves (Kaplan-Meier style) and cumulative revenue per acquired line.
-- Materialization: table (every cohort's whole curve is restated when a month closes).
-- Grain  : cohort_month x contract_type x cohort_age_months
-- Sources: fct_p03_cohort_month, int_p03_cohort_members (cohort sizes)
-- Survival(t) = product over ages <= t of (1 - churned / at_risk), computed as exp(sum(ln(...))).
with sizes as (
    select cohort_month, contract_type, count(*) as cohort_lines
    from {{ ref('int_p03_cohort_members') }} group by 1, 2
),
curve as (
    select cohort_month, contract_type, cohort_age_months, activity_month,
           sum(lines_observed)                                           as at_risk,
           sum(lines_churned)                                            as churned,
           sum(lines_in_base_eom)                                        as in_base,
           sum(revenue_php)                                              as revenue_php
    from {{ ref('fct_p03_cohort_month') }}
    group by 1, 2, 3, 4
)
select
    c.cohort_month,
    c.contract_type,
    c.cohort_age_months,
    c.activity_month,
    s.cohort_lines,
    c.at_risk,
    c.churned,
    c.in_base,
    round(c.churned / nullif(c.at_risk, 0), 5)                           as monthly_churn_hazard,
    round(exp(sum(ln(greatest(1 - c.churned / nullif(c.at_risk, 0), 1e-9)))
          over (partition by c.cohort_month, c.contract_type order by c.cohort_age_months
                rows between unbounded preceding and current row)), 5)   as survival_rate,
    round(c.in_base / nullif(s.cohort_lines, 0), 5)                      as observed_retention,
    round(c.revenue_php, 2)                                              as revenue_php,
    round(sum(c.revenue_php) over (partition by c.cohort_month, c.contract_type order by c.cohort_age_months
                                   rows between unbounded preceding and current row)
          / nullif(s.cohort_lines, 0), 2)                                as cumulative_revenue_per_line_php
from curve c
join sizes s on s.cohort_month = c.cohort_month and s.contract_type = c.contract_type
