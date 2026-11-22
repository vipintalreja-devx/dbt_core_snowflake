{{ config(
    materialized='incremental',
    schema='gold',
    tags=['insights', 'p10'],
    incremental_strategy='delete+insert',
    unique_key='month_start',
    on_schema_change='append_new_columns',
    cluster_by=['month_start'],
    transient=false
) }}
-- P10 device lifecycle | output 3: usage and revenue of the handset fleet per month.
-- Grain   : month_start x region_id x manufacturer x device_generation x lifecycle_stage
-- Sources : fct_subscriber_usage_daily, fct_revenue_daily (month window), dim_subscriber_device_history
--           (device of record = current device of the line), mart_p10_device_price_trend, int_subscriber_profile
with usage as (
    select subscription_id, date_trunc('month', event_date)::date as month_start,
           sum(data_mb) as data_mb, sum(data_mb_5g) as data_mb_5g, sum(voice_min) as voice_min, count(*) as active_days
    from {{ ref('fct_subscriber_usage_daily') }}
    where {{ f2_month_window('event_date', 'month_start') }}
    group by 1, 2
),
rev as (
    select subscription_id, date_trunc('month', business_date)::date as month_start, sum(total_revenue_php) as revenue_php
    from {{ ref('fct_revenue_daily') }}
    where {{ f2_month_window('business_date', 'month_start') }}
    group by 1, 2
),
dev as (
    select h.subscription_id, h.device_manufacturer, h.device_generation, t.lifecycle_stage
    from {{ ref('dim_subscriber_device_history') }} h
    left join {{ ref('mart_p10_device_price_trend') }} t on t.device_id = h.device_id
    where h.is_current
)
select
    u.month_start,
    p.home_region_id                                        as region_id,
    p.home_region_name                                      as region_name,
    coalesce(d.device_manufacturer, 'UNKNOWN')              as manufacturer,
    coalesce(d.device_generation, 'UNKNOWN')                as device_generation,
    coalesce(d.lifecycle_stage, 'UNKNOWN')                  as lifecycle_stage,
    count(*)                                                as active_lines,
    round(sum(u.data_mb) / 1024, 2)                         as data_gb,
    round(sum(u.data_mb_5g) / nullif(sum(u.data_mb), 0), 4) as data_5g_share,
    round(sum(u.voice_min), 1)                              as voice_min,
    round(avg(u.active_days), 1)                            as avg_active_days,
    round(sum(coalesce(r.revenue_php, 0)), 2)               as revenue_php,
    round(sum(coalesce(r.revenue_php, 0)) / count(*), 2)    as arpu_php,
    current_timestamp()                                     as loaded_at
from usage u
join {{ ref('int_subscriber_profile') }} p on p.subscription_id = u.subscription_id and p.is_reportable
left join dev d on d.subscription_id = u.subscription_id
left join rev r on r.subscription_id = u.subscription_id and r.month_start = u.month_start
group by 1, 2, 3, 4, 5, 6
