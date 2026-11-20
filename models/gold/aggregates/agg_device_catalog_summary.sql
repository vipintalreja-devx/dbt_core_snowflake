{{ config(
    materialized='table',
    tags=['gold', 'aggregates', 'ref', 'device', 'snapshot', 'e24']
) }}
-- Device catalogue per manufacturer x device type (launched to date) with installed base on the network.
-- Ported from x_e24_device_catalog_summary (PIPE_X.SP_X_E24_DEVICE_CATALOG_SUMMARY).
-- Grain   : manufacturer x device_type   (current state)
-- Sources : slv_device, int_subscriber_profile (installed base), slv_plan
with base as (
    select p.device_id,
           count_if(p.status in ('ACTIVE', 'SUSPENDED'))    as active_lines,
           sum(iff(p.status in ('ACTIVE', 'SUSPENDED'), p.monthly_charges_php, 0)) as active_mrr_php,
           count_if(p.status in ('ACTIVE', 'SUSPENDED') and pl.speed_mbps >= 100) as active_lines_high_speed_plan
    from {{ ref('int_subscriber_profile') }} p
    left join {{ ref('slv_plan') }} pl on pl.plan_id = p.plan_id
    group by 1
),
d as (
    select dv.*, zeroifnull(b.active_lines) as active_lines, zeroifnull(b.active_mrr_php) as active_mrr_php,
           zeroifnull(b.active_lines_high_speed_plan) as active_lines_high_speed_plan
    from {{ ref('slv_device') }} dv
    left join base b on b.device_id = dv.device_id
    where dv.launch_date <= current_date or dv.launch_date is null
)
select
    current_date                                            as snapshot_date,
    manufacturer, device_type,
    count(*)                                                as models,
    count_if(supports_5g)                                   as models_5g,
    round(avg(list_price_php), 0)                           as avg_price_php,
    median(list_price_php)                                  as median_price_php,
    max_by(model, launch_date)                              as newest_model,
    max(launch_date)                                        as newest_launch_date,
    sum(active_lines)                                       as active_lines,
    sum(iff(supports_5g, active_lines, 0))                  as active_lines_5g_capable,
    round(div0(sum(iff(supports_5g, active_lines, 0)), sum(active_lines)) * 100, 2) as pct_base_5g_capable,
    round(div0(sum(active_mrr_php), sum(active_lines)), 2)  as arpu_proxy_php,
    sum(active_lines_high_speed_plan)                       as active_lines_high_speed_plan,
    round(ratio_to_report(sum(active_lines)) over (), 6)    as share_of_installed_base,
    current_timestamp()                                     as loaded_at
from d
group by 1, 2, 3
