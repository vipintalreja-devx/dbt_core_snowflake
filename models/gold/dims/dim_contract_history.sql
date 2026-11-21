{{ config(
    materialized='table',
    schema='gold',
    cluster_by=['is_current', 'region_name'],
    tags=['gold', 'dims', 'scd2', 'h01'],
    transient=false
) }}
-- Materialization: table (current-state/small reference): dimension (small, current state; history comes from the SCD2 snapshots).
-- SCD Type 2 contract dimension over snapshot snap_contract_profile.
-- Ported from telco_v2_core x_h01_dim_contract_scd2. Grain: contract x version.
-- Adds version sequencing, version duration and what changed vs. the previous version.
-- Sources: snap_contract_profile, dim_subscriber (current brand/region for comparison).
with versions as (
    select
        s.*,
        row_number() over (partition by s.contract_id order by s.dbt_valid_from) as version_no,
        lag(s.lifecycle_stage) over (partition by s.contract_id order by s.dbt_valid_from)  as prev_lifecycle_stage,
        lag(s.plan_id)         over (partition by s.contract_id order by s.dbt_valid_from)  as prev_plan_id,
        lag(s.price_band)      over (partition by s.contract_id order by s.dbt_valid_from)  as prev_price_band,
        lag(s.churn_band)      over (partition by s.contract_id order by s.dbt_valid_from)  as prev_churn_band
    from {{ ref('snap_contract_profile') }} s
)
select
    {{ dbt_utils.generate_surrogate_key(['v.contract_id', 'v.dbt_valid_from']) }} as contract_version_key,
    v.contract_id,
    v.subscription_id,
    v.account_id,
    v.version_no,
    v.contract_type,
    v.commitment_months,
    v.start_date,
    v.end_date,
    v.is_auto_renew,
    v.is_terminated,
    v.terminated_on,
    v.termination_reason,
    v.lifecycle_stage,
    v.months_left_band,
    v.plan_id,
    v.plan_name,
    v.is_plan_contract_mismatch,
    v.price_band,
    v.feature_count,
    v.subscription_status,
    v.payment_method,
    v.is_auto_pay,
    v.is_paperless_billing,
    v.device_manufacturer,
    v.brand,
    v.region_name,
    v.churn_band,
    array_compact(array_construct(
        iff(not equal_null(v.lifecycle_stage, v.prev_lifecycle_stage) and v.version_no > 1, 'LIFECYCLE', null),
        iff(not equal_null(v.plan_id, v.prev_plan_id)                 and v.version_no > 1, 'PLAN', null),
        iff(not equal_null(v.price_band, v.prev_price_band)           and v.version_no > 1, 'PRICE_BAND', null),
        iff(not equal_null(v.churn_band, v.prev_churn_band)           and v.version_no > 1, 'CHURN_BAND', null)
    ))                                                          as changed_attributes,
    v.prev_plan_id,
    d.brand_name                                                as current_brand_name,
    v.hash_diff,
    v.dbt_valid_from                                            as valid_from,
    coalesce(v.dbt_valid_to, '9999-12-31'::timestamp_ntz)       as valid_to,
    v.dbt_valid_to is null                                      as is_current,
    datediff('day', v.dbt_valid_from, coalesce(v.dbt_valid_to, current_timestamp()::timestamp_ntz)) as version_age_days
from versions v
left join {{ ref('dim_subscriber') }} d on d.subscription_id = v.subscription_id
