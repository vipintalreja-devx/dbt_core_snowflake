{{ config(
    materialized='table',
    schema='silver',
    transient=true,
    cluster_by=['cohort_month'],
    tags=['insights', 'p03', 'cohort']
) }}
-- P03 Cohort retention & LTV, step 1: which acquisition cohort and segment each line belongs to.
-- Materialization: table (current state, 600k lines).
-- Grain  : subscription_id
-- Sources: int_subscriber_profile (activation, plan, region, brand, device), dim_plan (base price)
select
    p.subscription_id,
    date_trunc('month', p.activation_date)::date                         as cohort_month,
    p.activation_date,
    p.deactivation_date,
    p.status,
    p.contract_type,
    p.plan_category,
    p.plan_id,
    coalesce(p.home_region_id, -1)                                       as region_id,
    coalesce(p.home_region_name, 'UNKNOWN')                              as region_name,
    p.island_group,
    p.brand_name,
    p.is_mvno_line,
    p.device_supports_5g,
    pl.base_price_php,
    p.contract_type || ' | ' || coalesce(p.plan_category, 'NA') || ' | ' || coalesce(p.island_group, 'NA') as ltv_segment
from {{ ref('int_subscriber_profile') }} p
left join {{ ref('dim_plan') }} pl on pl.plan_id = p.plan_id
where p.activation_date is not null and p.is_reportable
