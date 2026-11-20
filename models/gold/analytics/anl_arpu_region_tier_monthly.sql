{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='revenue_month',
    on_schema_change='append_new_columns',
    tags=['gold', 'analytics', 'revenue', 'arpu', 'm32']
) }}
-- Regional ARPU benchmarked against the national ARPU each month (ABOVE / AT / BELOW national, +/-10%),
-- with revenue-stream mix, rank and month-over-month movement.
-- Ported from x_m32_js_arpu_region (JavaScript procedure looping over a result set) as set-based SQL.
-- Grain: revenue_month x region_name.
-- Sources: fct_subscriber_monthly, int_subscriber_profile, slv_region.
with m as (
    select f.*, p.home_region_id, p.island_group
    from {{ ref('fct_subscriber_monthly') }} f
    left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = f.subscription_id
    where f.is_reportable
      and {{ f2_month_window('f.month_start', 'revenue_month') }}
),
r as (
    select
        month_start                                         as revenue_month,
        region_name,
        any_value(home_region_id)                           as region_id,
        any_value(island_group)                             as island_group,
        (count_if(is_in_base_bom) + count_if(is_in_base_eom)) / 2 as avg_base,
        sum(total_revenue_php)                              as revenue_php,
        sum(plan_revenue_php)                               as plan_revenue_php,
        sum(recharge_revenue_php)                           as recharge_revenue_php,
        sum(vas_revenue_php)                                as vas_revenue_php,
        sum(roaming_revenue_php)                            as roaming_revenue_php
    from m
    group by 1, 2
),
n as (
    select r.*,
           div0(r.revenue_php, r.avg_base)                  as arpu_php,
           div0(sum(r.revenue_php) over (partition by r.revenue_month),
                sum(r.avg_base) over (partition by r.revenue_month)) as national_arpu_php
    from r
)
select
    n.revenue_month,
    n.region_name,
    n.region_id,
    n.island_group,
    round(n.avg_base, 1)                                    as avg_base,
    round(n.revenue_php, 2)                                 as revenue_php,
    round(n.arpu_php, 2)                                    as arpu_php,
    round(n.national_arpu_php, 2)                           as national_arpu_php,
    round(div0(n.arpu_php, n.national_arpu_php), 4)         as arpu_index,
    case when n.arpu_php >= n.national_arpu_php * 1.1 then 'ABOVE_NATIONAL'
         when n.arpu_php <= n.national_arpu_php * 0.9 then 'BELOW_NATIONAL'
         else 'AT_NATIONAL' end                             as arpu_tier,
    round(div0(n.plan_revenue_php, n.revenue_php), 4)       as plan_share,
    round(div0(n.recharge_revenue_php, n.revenue_php), 4)   as recharge_share,
    round(div0(n.vas_revenue_php, n.revenue_php), 4)        as vas_share,
    round(div0(n.roaming_revenue_php, n.revenue_php), 4)    as roaming_share,
    dense_rank() over (partition by n.revenue_month order by n.arpu_php desc) as arpu_rank,
    round(n.arpu_php - lag(n.arpu_php) over (partition by n.region_name order by n.revenue_month), 2) as arpu_mom_change_php,
    current_timestamp()                                     as loaded_at
from n
