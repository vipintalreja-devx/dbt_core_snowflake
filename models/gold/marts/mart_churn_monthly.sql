{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='month_start',
    on_schema_change='append_new_columns',
    schema='gold',
    tags=['gold', 'marts', 'kpi'],
    transient=false,
    copy_grants=true
) }}
-- GOVERNED CHURN. Base movement per month from the BSS status history:
--   opening base + gross adds - churn = closing base (+ net other movements)
--   churn rate = churned lines / opening base
-- (replaces MART.V_CHURN_BILLING - "no next invoice" - and V_CHURN_CONTRACT - contract termination.)
select
    month_start,
    region_name,
    contract_type,
    count_if(is_in_base_bom)                                as opening_base,
    count_if(is_gross_add)                                  as gross_adds,
    count_if(is_churned)                                    as churned_lines,
    count_if(is_in_base_eom)                                as closing_base,
    count_if(is_in_base_eom) - count_if(is_in_base_bom)     as net_adds,
    round(div0(count_if(is_churned), count_if(is_in_base_bom)) * 100, 3) as churn_rate_pct,
    sum(iff(is_churned, total_revenue_php, 0))              as churned_revenue_php
from {{ ref('fct_subscriber_monthly') }}
where is_reportable
  and {{ f2_month_window('month_start', 'month_start') }}
group by 1, 2, 3
