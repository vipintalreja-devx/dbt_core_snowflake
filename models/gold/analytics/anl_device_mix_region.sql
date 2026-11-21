{{ config(
    materialized='table',
    tags=['gold', 'analytics', 'devices', 'm12']
) }}
-- Materialization: table (current-state/small reference): current-state profile of the subscriber/device base.
-- Device mix of the active base per home region and manufacturer: 5G readiness, device price tier,
-- device age and the data appetite of each segment (last closed month).
-- Ported from x_m12_device_mix_region (PIPE_X.SP_X_M12_DEVICE_MIX_REGION).
-- Grain: region_name x manufacturer x supports_5g.
-- Sources: int_subscriber_profile, slv_device, dim_device, fct_subscriber_monthly.
with last_month as (
    select max(month_start) as month_start from {{ ref('fct_subscriber_monthly') }}
),
usage as (
    select subscription_id, data_mb, data_mb_5g, total_revenue_php
    from {{ ref('fct_subscriber_monthly') }}
    where month_start = (select month_start from last_month)
),
base as (
    select
        p.home_region_name,
        coalesce(d.manufacturer, 'UNKNOWN')                 as manufacturer,
        coalesce(d.supports_5g, false)                      as supports_5g,
        dd.price_tier,
        d.list_price_php,
        datediff('month', d.launch_date, (select month_start from last_month)) as device_age_months,
        u.data_mb,
        u.data_mb_5g,
        u.total_revenue_php
    from {{ ref('int_subscriber_profile') }} p
    left join {{ ref('slv_device') }} d  on d.device_id = p.device_id
    left join {{ ref('dim_device') }} dd on dd.device_id = p.device_id
    left join usage u                    on u.subscription_id = p.subscription_id
    where p.is_reportable
      and p.status = 'ACTIVE'
)
select
    home_region_name                                        as region_name,
    manufacturer,
    supports_5g,
    (select month_start from last_month)                    as usage_month,
    count(*)                                                as active_lines,
    count_if(price_tier = 'PREMIUM')                        as premium_devices,
    count_if(price_tier = 'MID')                            as mid_devices,
    count_if(price_tier = 'ENTRY')                          as entry_devices,
    round(avg(list_price_php), 2)                           as avg_device_price_php,
    round(median(device_age_months), 1)                     as median_device_age_months,
    round(avg(data_mb) / 1024, 3)                           as avg_data_gb,
    round(div0(sum(data_mb_5g), sum(data_mb)), 4)           as share_of_data_on_5g,
    round(avg(total_revenue_php), 2)                        as avg_revenue_php,
    round(ratio_to_report(count(*)) over (partition by home_region_name), 4) as share_of_region_base,
    round(ratio_to_report(count(*)) over (partition by home_region_name, supports_5g), 4) as share_within_5g_class,
    current_timestamp()                                     as loaded_at
from base
group by 1, 2, 3
