{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'marts', 'kpi'],
    transient=false,
    copy_grants=true
) }}
-- GOVERNED ARPU. One definition for Finance, Marketing and Network
-- (replaces MART.V_ARPU_FINANCE / V_ARPU_MARKETING / V_ARPU_NETWORK, which disagree):
--   ARPU = earned revenue in month / average subscriber base ((BOM + EOM) / 2)
--   Base = reportable lines in status ACTIVE or SUSPENDED (billable per REF.STATUS_CODE)
select
    m.month_start,
    m.region_name,
    m.contract_type,
    count_if(m.is_in_base_bom)                              as base_bom,
    count_if(m.is_in_base_eom)                              as base_eom,
    (count_if(m.is_in_base_bom) + count_if(m.is_in_base_eom)) / 2 as base_avg,
    count_if(m.is_usage_active)                             as usage_active_lines,
    sum(m.total_revenue_php)                                as revenue_php,
    sum(m.plan_revenue_php)                                 as plan_revenue_php,
    sum(m.recharge_revenue_php)                             as recharge_revenue_php,
    sum(m.vas_revenue_php)                                  as vas_revenue_php,
    sum(m.roaming_revenue_php)                              as roaming_revenue_php,
    round(div0(sum(m.total_revenue_php), (count_if(m.is_in_base_bom) + count_if(m.is_in_base_eom)) / 2), 2) as arpu_php,
    round(div0(sum(m.data_mb), count_if(m.is_usage_active)) / 1024, 3)  as data_gb_per_active_line,
    round(div0(sum(m.voice_min), count_if(m.is_usage_active)), 1)       as mou_per_active_line
from {{ ref('fct_subscriber_monthly') }} m
where m.is_reportable
group by 1, 2, 3
