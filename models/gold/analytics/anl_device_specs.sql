{{ config(
    materialized='table',
    tags=['gold', 'analytics', 'devices', 'm23']
) }}
-- Materialization: table (current-state/small reference): current-state profile of the subscriber/device base.
-- Device specification catalogue parsed from the SPECS JSON (chipset, memory, bands, eSIM, VoLTE,
-- type approval) with the size of the active base on each model and a capability score.
-- Ported from x_m23_device_specs_flat (PIPE_X.SP_X_M23_DEVICE_SPECS_FLAT).
-- Grain: device_id.
-- Sources: slv_device, dim_device, int_subscriber_profile, fct_subscriber_monthly.
with base as (
    select p.device_id,
           count(*)                                         as active_lines,
           avg(m.data_mb)                                   as avg_data_mb_last_month
    from {{ ref('int_subscriber_profile') }} p
    left join {{ ref('fct_subscriber_monthly') }} m
           on m.subscription_id = p.subscription_id
          and m.month_start = (select max(month_start) from {{ ref('fct_subscriber_monthly') }})
    where p.is_reportable and p.status = 'ACTIVE'
    group by 1
)
select
    d.device_id,
    d.manufacturer,
    d.model,
    d.device_type,
    d.supports_5g,
    d.launch_date,
    d.list_price_php,
    dd.price_tier,
    d.specs:chipset::string                                 as chipset,
    d.specs:ram_gb::number                                  as ram_gb,
    d.specs:storage_gb::float                               as storage_gb,
    d.specs:battery_mah::number                             as battery_mah,
    d.specs:screen_in::number(4, 1)                         as screen_in,
    coalesce(d.specs:esim::boolean, false)                  as has_esim,
    coalesce(d.specs:volte::boolean, false)                 as has_volte,
    d.specs:firmware::string                                as firmware,
    d.specs:tac::string                                     as tac,
    d.specs:ntc_type_approval::string                       as ntc_type_approval,
    coalesce(array_size(d.specs:bands), 0)                  as band_count,
    array_to_string(d.specs:bands, ',')                     as bands,
    array_contains('n78'::variant, d.specs:bands)           as supports_n78,
    d.specs:ntc_type_approval is null                       as is_missing_type_approval,
    -- simple capability score 0..100 used by device-upgrade campaigns
    least(100, round(
          iff(d.supports_5g, 30, 0)
        + iff(coalesce(d.specs:volte::boolean, false), 15, 0)
        + iff(coalesce(d.specs:esim::boolean, false), 10, 0)
        + least(coalesce(d.specs:ram_gb::number, 0), 12) * 2.5
        + least(coalesce(array_size(d.specs:bands), 0), 15)
    , 0))                                                   as capability_score,
    zeroifnull(b.active_lines)                              as active_lines,
    round(b.avg_data_mb_last_month / 1024, 3)               as avg_data_gb_last_month,
    current_timestamp()                                     as loaded_at
from {{ ref('slv_device') }} d
left join {{ ref('dim_device') }} dd on dd.device_id = d.device_id
left join base b on b.device_id = d.device_id
