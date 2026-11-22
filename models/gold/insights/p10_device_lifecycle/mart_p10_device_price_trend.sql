{{ config(
    materialized='table',
    schema='gold',
    tags=['insights', 'p10'],
    transient=false,
    copy_grants=true
) }}
-- P10 device lifecycle | output 1: lifecycle stage and price trend of every handset model.
-- Grain   : device_id
-- Sources : dim_device, int_p10_device_price_timeline, dim_subscriber_device_history (installed base)
with px as (
    select device_id,
           count_if(price_source = 'VENDOR_FEED')           as vendor_observations,
           min(iff(price_source = 'VENDOR_FEED', price_date, null)) as first_vendor_date,
           max(iff(price_source = 'VENDOR_FEED', price_date, null)) as last_vendor_date,
           max_by(price_php, price_date)                    as latest_price_php,
           regr_slope(price_php, datediff('day', '2000-01-01'::date, price_date)) * 30 as price_slope_per_30d,
           stddev_samp(iff(price_source = 'VENDOR_FEED', price_php, null)) as vendor_price_stddev
    from {{ ref('int_p10_device_price_timeline') }}
    group by 1
),
base as (
    select device_id, count(*) as installed_lines,
           count_if(is_esim) as esim_lines
    from {{ ref('dim_subscriber_device_history') }}
    where is_current
    group by 1
)
select
    d.device_id, d.manufacturer, d.model, d.device_type, d.supports_5g, d.launch_date, d.price_tier,
    d.list_price_php                                        as launch_price_php,
    p.latest_price_php,
    round(p.latest_price_php / nullif(d.list_price_php, 0) - 1, 4) as price_change_since_launch,
    round(p.price_slope_per_30d, 2)                         as price_slope_per_30d,
    round(p.vendor_price_stddev, 2)                         as vendor_price_stddev,
    coalesce(p.vendor_observations, 0)                      as vendor_observations,
    p.first_vendor_date, p.last_vendor_date,
    datediff('month', d.launch_date, current_date())        as months_since_launch,
    case
        when datediff('month', d.launch_date, current_date()) < 12 then 'NEW'
        when datediff('month', d.launch_date, current_date()) < 36 then 'MAINSTREAM'
        when datediff('month', d.launch_date, current_date()) < 60 then 'LEGACY'
        else 'END_OF_LIFE'
    end                                                     as lifecycle_stage,
    coalesce(b.installed_lines, 0)                          as installed_lines,
    coalesce(b.esim_lines, 0)                               as esim_lines,
    current_timestamp()                                     as loaded_at
from {{ ref('dim_device') }} d
left join px p on p.device_id = d.device_id
left join base b on b.device_id = d.device_id
