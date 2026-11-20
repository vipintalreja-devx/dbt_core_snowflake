{{ config(
    tags=['gold', 'facts', 'fact'],
    schema='gold',
    materialized='incremental',
    unique_key='business_date',
    incremental_strategy='delete+insert',
    cluster_by=['business_date'],
    on_schema_change='append_new_columns'
) }}
-- Grain: line x business day. Earned revenue by stream (PLAN / RECHARGE / VAS / ROAMING), PHP.
select
    subscription_id,
    business_date,
    sum(iff(revenue_source = 'PLAN',     revenue_php, 0))   as plan_revenue_php,
    sum(iff(revenue_source = 'RECHARGE', revenue_php, 0))   as recharge_revenue_php,
    sum(iff(revenue_source = 'VAS',      revenue_php, 0))   as vas_revenue_php,
    sum(iff(revenue_source = 'ROAMING',  revenue_php, 0))   as roaming_revenue_php,
    sum(revenue_php)                                        as total_revenue_php
from {{ ref('slv_daily_revenue') }}
where {{ incremental_date_filter('business_date') }}
group by 1, 2
