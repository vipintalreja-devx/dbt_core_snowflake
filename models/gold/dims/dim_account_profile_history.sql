{{ config(
    materialized='table',
    schema='gold',
    cluster_by=['is_current', 'value_band'],
    tags=['gold', 'dims', 'scd2', 'h04'],
    transient=false
) }}
-- Materialization: table (current-state/small reference): dimension (small, current state; history comes from the SCD2 snapshots).
-- SCD Type 2 account profile dimension over snapshot snap_account_profile.
-- Ported from telco_v2_core x_h04_dim_account_profile_scd2. Grain: account x version.
-- Sources: snap_account_profile, dim_account (current line counts), dim_customer (age band).
with versions as (
    select
        s.*,
        row_number() over (partition by s.account_id order by s.dbt_valid_from)              as version_no,
        lag(s.value_band)         over (partition by s.account_id order by s.dbt_valid_from) as prev_value_band,
        lag(s.payment_discipline) over (partition by s.account_id order by s.dbt_valid_from) as prev_payment_discipline,
        lag(s.household_band)     over (partition by s.account_id order by s.dbt_valid_from) as prev_household_band
    from {{ ref('snap_account_profile') }} s
)
select
    {{ dbt_utils.generate_surrogate_key(['v.account_id', 'v.dbt_valid_from']) }} as account_version_key,
    v.account_id,
    v.customer_id,
    v.version_no,
    v.payment_method,
    v.is_paperless_billing,
    v.billing_cycle_day,
    v.language_name,
    v.kyc_level,
    v.granted_consents,
    v.consent_channel_count,
    v.total_lines,
    v.active_lines,
    v.mvno_lines,
    v.fixed_services,
    v.ftth_services,
    v.household_band,
    v.main_plan_id,
    v.main_plan_name,
    v.main_segment,
    v.main_contract_type,
    v.payment_discipline,
    v.avg_days_vs_due,
    v.avg_monthly_revenue_php,
    v.value_band,
    dc.age_band,
    dc.has_marketing_consent,
    da.active_lines                                             as current_active_lines,
    case
        when v.version_no = 1 then 'INITIAL'
        when decode(v.value_band, 'HIGH', 3, 'MEDIUM', 2, 'LOW', 1, 0)
           > decode(v.prev_value_band, 'HIGH', 3, 'MEDIUM', 2, 'LOW', 1, 0) then 'VALUE_UP'
        when decode(v.value_band, 'HIGH', 3, 'MEDIUM', 2, 'LOW', 1, 0)
           < decode(v.prev_value_band, 'HIGH', 3, 'MEDIUM', 2, 'LOW', 1, 0) then 'VALUE_DOWN'
        when not equal_null(v.payment_discipline, v.prev_payment_discipline) then 'PAYMENT_BEHAVIOUR'
        when not equal_null(v.household_band, v.prev_household_band)         then 'HOUSEHOLD'
        else 'PROFILE'
    end                                                         as change_type,
    v.dbt_valid_from                                            as valid_from,
    coalesce(v.dbt_valid_to, '9999-12-31'::timestamp_ntz)       as valid_to,
    v.dbt_valid_to is null                                      as is_current
from versions v
left join {{ ref('dim_customer') }} dc on dc.customer_id = v.customer_id
left join {{ ref('dim_account') }} da  on da.account_id = v.account_id
