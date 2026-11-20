{% snapshot snap_subscriber_device %}
{{ config(
    unique_key='subscription_id',
    strategy='check',
    check_cols=['device_id', 'sim_iccid', 'sim_type', 'partner_brand_id', 'device_generation', 'network_fit'],
    invalidate_hard_deletes=True,
    tags=['snapshot', 'scd2', 'device', 'h02']
) }}
{#-
  Subscriber device & SIM profile, SCD Type 2 (check strategy on explicit columns).
  Ported from telco_v2_core snapshot x_h02_dim_subscriber_device_scd2 (PIPE_X.SP_X_H02).
  A new version is written when the line's handset, SIM card, SIM type, MVNO brand, device
  generation or radio "network fit" changes (device swap, eSIM migration, 5G upgrade).
  Grain: one row per subscription (current state).
  Sources (10): slv_subscription, slv_device (SPECS JSON), slv_sim_inventory, slv_partner_brand,
  slv_partner_subscriber_map, slv_plan, slv_cell_site, slv_network_element, slv_account, slv_customer.
-#}
with params as (
    select max(valid_from_date) as as_of_date from {{ ref('slv_subscription_status_history') }}
),
device as (
    select
        d.device_id, d.manufacturer, d.model, d.device_type, d.supports_5g, d.launch_date, d.list_price_php,
        try_to_number(d.specs:battery_mah::string)              as battery_mah,
        d.specs:chipset::string                                 as chipset,
        coalesce(d.specs:esim::boolean, false)                  as device_esim_capable,
        coalesce(d.specs:volte::boolean, false)                 as device_volte,
        try_to_number(d.specs:ram_gb::string)                   as ram_gb,
        try_to_double(d.specs:storage_gb::string)               as storage_gb,
        array_size(d.specs:firmware)                            as firmware_releases,
        d.specs:firmware[array_size(d.specs:firmware) - 1]:v::string as latest_firmware
    from {{ ref('slv_device') }} d
),
device_bands as (
    -- radio bands supported by the handset, from the SPECS.bands JSON array
    select
        d.device_id,
        count(distinct b.value::string)                         as band_count,
        count_if(startswith(b.value::string, 'n'))              as nr_bands,
        count_if(startswith(b.value::string, 'b'))              as lte_bands,
        max(iff(b.value::string = 'n78', 1, 0))                 as has_n78,
        array_agg(distinct b.value::string) within group (order by b.value::string) as band_list
    from {{ ref('slv_device') }} d,
         lateral flatten(input => d.specs:bands) b
    where b.value is not null and typeof(b.value) = 'VARCHAR'
    group by d.device_id
),
sim as (
    select msisdn, iccid, sim_type, sim_status, activated_at_utc
    from {{ ref('slv_sim_inventory') }}
    where msisdn is not null
    qualify row_number() over (partition by msisdn order by activated_at_utc desc nulls last) = 1
),
home_cell as (
    select c.cell_id, c.radio_type, c.region_name,
           listagg(distinct ne.vendor, ',') within group (order by ne.vendor) as cell_vendors
    from {{ ref('slv_cell_site') }} c
    left join {{ ref('slv_network_element') }} ne on ne.cell_id = c.cell_id
    group by c.cell_id, c.radio_type, c.region_name
),
customer as (
    select a.account_id, cu.preferred_language, cu.is_test_customer
    from {{ ref('slv_account') }} a
    join {{ ref('slv_customer') }} cu on cu.customer_id = a.customer_id
)
select
    s.subscription_id,
    s.account_id,
    s.msisdn,
    s.status,
    s.device_id,
    d.manufacturer                                              as device_manufacturer,
    d.model                                                     as device_model,
    d.device_type,
    d.supports_5g                                               as device_supports_5g,
    d.launch_date                                               as device_launch_date,
    d.list_price_php                                            as device_list_price_php,
    d.battery_mah,
    d.chipset,
    d.device_esim_capable,
    d.device_volte,
    d.ram_gb,
    d.storage_gb,
    d.latest_firmware,
    zeroifnull(db.band_count)                                   as band_count,
    zeroifnull(db.nr_bands)                                     as nr_bands,
    zeroifnull(db.lte_bands)                                    as lte_bands,
    zeroifnull(db.has_n78) = 1                                  as has_n78,
    array_to_string(db.band_list, ',')                          as band_list,
    case
        when d.supports_5g                                      then '5G'
        when d.device_type in ('SMARTPHONE', 'TABLET')          then '4G'
        when d.device_type = 'FEATURE_PHONE'                    then 'LEGACY'
        when d.device_type in ('ROUTER', 'MODEM', 'ONT')        then 'FIXED'
        when d.device_type is null                              then 'UNKNOWN'
        else 'OTHER'
    end                                                         as device_generation,
    case
        when d.launch_date is null                                       then 'UNKNOWN'
        when datediff('year', d.launch_date, p.as_of_date) < 2           then 'CURRENT'
        when datediff('year', d.launch_date, p.as_of_date) < 4           then 'AGEING'
        else 'OLD'
    end                                                         as device_age_band,
    sm.iccid                                                    as sim_iccid,
    sm.sim_type,
    sm.sim_status,
    sm.activated_at_utc                                         as sim_activated_at_utc,
    equal_null(sm.sim_type, 'ESIM')                             as is_esim,
    sm.sim_status = 'RETIRED' and s.status = 'ACTIVE'           as is_retired_sim_on_active_line,
    s.partner_brand_id,
    coalesce(pb.brand_name, 'OWN_BRAND')                        as brand_name,
    pb.operator_type,
    pb.revenue_share_pct,
    pm.wholesale_plan,
    s.plan_id,
    pl.plan_name,
    pl.speed_mbps                                               as plan_speed_mbps,
    hc.radio_type                                               as home_radio_type,
    coalesce(hc.region_name, 'UNKNOWN')                         as home_region_name,
    hc.cell_vendors                                             as home_cell_vendors,
    case
        when d.supports_5g and hc.radio_type = '5G'             then '5G_READY'
        when d.supports_5g and hc.radio_type <> '5G'            then '5G_DEVICE_NO_COVERAGE'
        when not d.supports_5g and hc.radio_type = '5G'         then 'UPGRADE_CANDIDATE'
        when hc.radio_type = '3G'                               then 'LEGACY_NETWORK'
        else 'STANDARD'
    end                                                         as network_fit,
    decode(lower(c.preferred_language), 'en', 'English', 'fil', 'Filipino', 'ceb', 'Cebuano',
           'ilo', 'Ilocano', 'Other')                           as language_name
from {{ ref('slv_subscription') }} s
cross join params p
left join device d                              on d.device_id = s.device_id
left join device_bands db                       on db.device_id = s.device_id
left join sim sm                                on sm.msisdn = s.msisdn
left join {{ ref('slv_partner_brand') }} pb     on pb.partner_brand_id = s.partner_brand_id
left join {{ ref('slv_partner_subscriber_map') }} pm on pm.subscription_id = s.subscription_id
left join {{ ref('slv_plan') }} pl              on pl.plan_id = s.plan_id
left join home_cell hc                          on hc.cell_id = s.home_cell_id
left join customer c                            on c.account_id = s.account_id
where not s.is_test
{% endsnapshot %}
