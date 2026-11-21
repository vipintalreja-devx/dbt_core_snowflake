{{ config(
    materialized='table',
    schema='gold',
    cluster_by=['is_current', 'device_generation'],
    tags=['gold', 'dims', 'scd2', 'h02'],
    transient=false
) }}
-- Materialization: table (current-state/small reference): dimension (small, current state; history comes from the SCD2 snapshots).
-- SCD Type 2 subscriber device / SIM dimension over snapshot snap_subscriber_device.
-- Ported from telco_v2_core x_h02_dim_subscriber_device_scd2. Grain: line x version.
-- Classifies each version change (device swap, SIM swap, eSIM migration, 5G upgrade, brand move).
-- Sources: snap_subscriber_device, dim_device (price tier), dim_region (island group).
with versions as (
    select
        s.*,
        row_number() over (partition by s.subscription_id order by s.dbt_valid_from)                as version_no,
        lag(s.device_id)         over (partition by s.subscription_id order by s.dbt_valid_from)    as prev_device_id,
        lag(s.sim_iccid)         over (partition by s.subscription_id order by s.dbt_valid_from)    as prev_sim_iccid,
        lag(s.sim_type)          over (partition by s.subscription_id order by s.dbt_valid_from)    as prev_sim_type,
        lag(s.device_generation) over (partition by s.subscription_id order by s.dbt_valid_from)    as prev_device_generation,
        lag(s.partner_brand_id)  over (partition by s.subscription_id order by s.dbt_valid_from)    as prev_partner_brand_id
    from {{ ref('snap_subscriber_device') }} s
)
select
    {{ dbt_utils.generate_surrogate_key(['v.subscription_id', 'v.dbt_valid_from']) }} as device_version_key,
    v.subscription_id,
    v.account_id,
    v.version_no,
    v.device_id,
    v.device_manufacturer,
    v.device_model,
    v.device_type,
    v.device_generation,
    v.device_age_band,
    v.device_supports_5g,
    v.band_count,
    v.nr_bands,
    v.has_n78,
    dd.price_tier                                               as device_price_tier,
    v.sim_iccid,
    v.sim_type,
    v.is_esim,
    v.is_retired_sim_on_active_line,
    v.brand_name,
    v.home_region_name,
    rg.island_group,
    v.home_radio_type,
    v.network_fit,
    case
        when v.version_no = 1                                                      then 'INITIAL'
        when v.prev_sim_type = 'USIM' and v.sim_type = 'ESIM'                      then 'ESIM_MIGRATION'
        when v.prev_device_generation <> '5G' and v.device_generation = '5G'       then 'UPGRADE_TO_5G'
        when not equal_null(v.prev_device_id, v.device_id)                         then 'DEVICE_SWAP'
        when not equal_null(v.prev_sim_iccid, v.sim_iccid)                         then 'SIM_SWAP'
        when not equal_null(v.prev_partner_brand_id, v.partner_brand_id)           then 'BRAND_MOVE'
        else 'ATTRIBUTE_CHANGE'
    end                                                         as change_type,
    v.dbt_valid_from                                            as valid_from,
    coalesce(v.dbt_valid_to, '9999-12-31'::timestamp_ntz)       as valid_to,
    v.dbt_valid_to is null                                      as is_current
from versions v
left join {{ ref('dim_device') }} dd on dd.device_id = v.device_id
left join {{ ref('dim_region') }} rg on rg.region_name = v.home_region_name
