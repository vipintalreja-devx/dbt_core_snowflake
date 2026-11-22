{{ config(
    materialized='table',
    schema='gold',
    tags=['insights', 'p10'],
    transient=false,
    cluster_by=['region_id'],
    copy_grants=true
) }}
-- P10 device lifecycle | output 2: handset upgrade propensity per active line (current state).
-- Grain   : subscription_id
-- Sources : dim_subscriber_device_history (current device, SIM swaps), mart_p10_device_price_trend,
--           int_subscriber_profile, slv_cell_site (home cell radio), fct_subscriber_usage_daily and
--           fct_revenue_daily (last 90 days, literal date bound so partitions are pruned)
-- Score   : logistic of a weighted sum of device age, 5G gap, usage growth, ARPU, contract end,
--           churn band and price drop of the current handset; bands HIGH >= 0.6, MEDIUM >= 0.35.
{% set from_90 = p_max_date_literal(ref('fct_subscriber_usage_daily'), 'event_date', 89) %}
{% set from_30 = p_max_date_literal(ref('fct_subscriber_usage_daily'), 'event_date', 29) %}
{% set rev_from = p_max_date_literal(ref('fct_revenue_daily'), 'business_date', 89) %}
with usage as (
    select subscription_id,
           sum(data_mb)                                     as data_mb_90d,
           sum(iff(event_date >= {{ from_30 }}, data_mb, 0)) as data_mb_30d,
           sum(iff(event_date <  {{ from_30 }}, data_mb, 0)) as data_mb_prior_60d,
           sum(data_mb_5g)                                  as data_mb_5g_90d,
           sum(voice_min)                                   as voice_min_90d,
           count(*)                                         as active_days_90d
    from {{ ref('fct_subscriber_usage_daily') }}
    where event_date >= {{ from_90 }}
    group by 1
),
rev as (
    select subscription_id, sum(total_revenue_php) / 3 as arpu_90d_php
    from {{ ref('fct_revenue_daily') }}
    where business_date >= {{ rev_from }}
    group by 1
),
dev as (
    select subscription_id, device_id, device_manufacturer, device_model, device_generation,
           device_supports_5g, device_age_band, is_esim
    from {{ ref('dim_subscriber_device_history') }}
    where is_current
),
swaps as (
    select subscription_id, count_if(change_type = 'SIM_SWAP') as sim_swaps_recent
    from {{ ref('dim_subscriber_device_history') }}
    group by 1
),
features as (
    select
        p.subscription_id,
        p.home_region_id                                    as region_id,
        p.home_region_name                                  as region_name,
        p.plan_category, p.contract_type, p.churn_band, p.tenure_months, p.contract_end_date,
        d.device_id, d.device_manufacturer, d.device_model, d.device_generation, d.device_supports_5g, d.is_esim,
        t.lifecycle_stage, t.months_since_launch, t.price_change_since_launch,
        c.radio_type                                        as home_cell_radio_type,
        coalesce(u.data_mb_90d, 0)                          as data_mb_90d,
        round(coalesce(u.data_mb_5g_90d, 0) / nullif(u.data_mb_90d, 0), 4) as data_5g_share_90d,
        round((coalesce(u.data_mb_30d, 0) * 2) / nullif(u.data_mb_prior_60d, 0) - 1, 4) as data_growth_30d_vs_prior,
        coalesce(u.active_days_90d, 0)                      as active_days_90d,
        round(coalesce(r.arpu_90d_php, 0), 2)               as arpu_90d_php,
        coalesce(s.sim_swaps_recent, 0)                     as sim_swaps_recent,
        datediff('day', current_date(), p.contract_end_date) between 0 and 90 as contract_ends_90d
    from {{ ref('int_subscriber_profile') }} p
    join dev d on d.subscription_id = p.subscription_id
    left join {{ ref('mart_p10_device_price_trend') }} t on t.device_id = d.device_id
    left join {{ ref('slv_cell_site') }} c on c.cell_id = p.home_cell_id
    left join usage u on u.subscription_id = p.subscription_id
    left join rev r on r.subscription_id = p.subscription_id
    left join swaps s on s.subscription_id = p.subscription_id
    where p.status = 'ACTIVE' and p.is_reportable
),
scored as (
    select f.*,
           (f.home_cell_radio_type = '5G' and not f.device_supports_5g) as is_5g_gap,
           {{ p_sigmoid("-2.2"
               ~ " + 0.035 * least(coalesce(f.months_since_launch, 0), 72)"
               ~ " + iff(f.home_cell_radio_type = '5G' and not f.device_supports_5g, 1.1, 0)"
               ~ " + 0.8 * least(greatest(coalesce(f.data_growth_30d_vs_prior, 0), -1), 2)"
               ~ " + iff(f.arpu_90d_php >= 800, 0.5, 0)"
               ~ " + iff(f.contract_ends_90d, 0.9, 0)"
               ~ " + iff(f.churn_band = 'HIGH', -0.4, 0)"
               ~ " + iff(coalesce(f.price_change_since_launch, 0) <= -0.2, 0.3, 0)") }} as upgrade_propensity
    from features f
)
select
    s.*,
    case when s.upgrade_propensity >= 0.6 then 'HIGH' when s.upgrade_propensity >= 0.35 then 'MEDIUM' else 'LOW' end as propensity_band,
    case
        when s.is_5g_gap and s.upgrade_propensity >= 0.35                  then 'OFFER_5G_HANDSET'
        when s.contract_ends_90d and s.upgrade_propensity >= 0.35          then 'RECONTRACT_WITH_DEVICE'
        when s.lifecycle_stage in ('LEGACY', 'END_OF_LIFE') and s.upgrade_propensity >= 0.6  then 'TRADE_IN_OFFER'
        else 'NO_ACTION'
    end                                                     as recommended_offer,
    current_timestamp()                                     as loaded_at
from scored s
