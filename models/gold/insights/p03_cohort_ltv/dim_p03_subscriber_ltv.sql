{{ config(
    materialized='table',
    schema='gold',
    transient=false,
    cluster_by=['region_id'],
    copy_grants=true,
    tags=['insights', 'p03', 'ltv']
) }}
-- P03 Projected remaining lifetime value per active line.
-- Materialization: table (current state).
-- Grain  : subscription_id (lines in base at the latest month)
-- Sources: int_p03_cohort_members, mart_p03_ltv_segment, fct_subscriber_monthly (last 3 months),
--          int_subscriber_profile (individual churn probability adjusts the segment hazard)
with recent as (
    select subscription_id, avg(total_revenue_php) as arpu_3m_php, max_by(is_in_base_eom, month_start) as in_base_now
    from {{ ref('fct_subscriber_monthly') }}
    where month_start >= dateadd('month', -3, (select max(month_start) from {{ ref('fct_subscriber_monthly') }}))
    group by 1
)
select
    m.subscription_id,
    m.region_id,
    m.region_name,
    m.cohort_month,
    m.ltv_segment,
    datediff('month', m.cohort_month, current_date())                    as tenure_months,
    round(r.arpu_3m_php, 2)                                              as arpu_3m_php,
    s.monthly_churn_hazard                                               as segment_hazard,
    p.churn_probability,
    -- individual hazard: segment hazard scaled by the line's churn score relative to 0.5 (bounded)
    least(0.5, greatest(0.001, s.monthly_churn_hazard * (0.5 + coalesce(p.churn_probability, 0.5)))) as line_hazard,
    round(r.arpu_3m_php * {{ var('insights_margin_pct', 0.35) }}
          * (1 - least(0.5, greatest(0.001, s.monthly_churn_hazard * (0.5 + coalesce(p.churn_probability, 0.5)))))
          / (1 + 0.10 / 12 - (1 - least(0.5, greatest(0.001, s.monthly_churn_hazard * (0.5 + coalesce(p.churn_probability, 0.5)))))), 2) as remaining_ltv_php,
    ntile(10) over (order by r.arpu_3m_php * (1 - least(0.5, greatest(0.001, s.monthly_churn_hazard * (0.5 + coalesce(p.churn_probability, 0.5)))))
                    / (1 + 0.10 / 12 - (1 - least(0.5, greatest(0.001, s.monthly_churn_hazard * (0.5 + coalesce(p.churn_probability, 0.5))))))) as ltv_decile
from {{ ref('int_p03_cohort_members') }} m
join recent r on r.subscription_id = m.subscription_id and r.in_base_now
left join {{ ref('mart_p03_ltv_segment') }} s on s.ltv_segment = m.ltv_segment
left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = m.subscription_id
