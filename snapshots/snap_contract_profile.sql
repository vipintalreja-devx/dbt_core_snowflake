{% snapshot snap_contract_profile %}
{{ config(
    unique_key='contract_id',
    strategy='check',
    check_cols=['hash_diff'],
    invalidate_hard_deletes=True,
    tags=['snapshot', 'scd2', 'contract', 'h01']
) }}
{#-
  Contract profile, SCD Type 2 (check strategy on HASH_DIFF).
  Ported from telco_v2_core snapshot x_h01_dim_contract_scd2 (PIPE_X.SP_X_H01_DIM_CONTRACT_SCD2),
  repointed from DBT_V2_RAW to the silver layer and evaluated as of the latest BSS status date.
  Grain: one row per contract (current state); dbt versions it when banded attributes change.
  Volatile measures (usage, revenue) are deliberately excluded so only attribute changes version.
  Sources (10): slv_contract, slv_subscription, slv_subscription_status_history, slv_plan,
  slv_account, slv_customer, slv_device, slv_partner_brand, slv_cell_site, slv_churn_score.
-#}
with params as (
    select max(valid_from_date) as as_of_date
    from {{ ref('slv_subscription_status_history') }}
),
contracts as (
    select c.*
    from {{ ref('slv_contract') }} c
    cross join params p
    where c.start_date <= p.as_of_date
),
status_as_of as (
    select h.subscription_id,
           max_by(h.status, h.valid_from_local)                as subscription_status,
           max(h.valid_from_date)                              as status_since,
           count(*)                                            as status_changes
    from {{ ref('slv_subscription_status_history') }} h
    cross join params p
    where h.valid_from_date <= p.as_of_date
    group by h.subscription_id
),
account as (
    select a.account_id, a.payment_method, a.is_paperless_billing, a.billing_cycle_day,
           cu.customer_code, cu.is_senior, cu.is_test_customer
    from {{ ref('slv_account') }} a
    left join {{ ref('slv_customer') }} cu on cu.customer_id = a.customer_id
),
assembled as (
    select
        c.contract_id,
        c.subscription_id,
        s.account_id,
        a.customer_code,
        c.contract_type,
        c.commitment_months,
        c.start_date,
        c.end_date,
        c.is_auto_renew,
        c.terminated_on,
        c.termination_reason,
        c.is_terminated,
        case
            when c.is_terminated                                       then 'TERMINATED'
            when c.end_date < p.as_of_date and c.is_auto_renew         then 'ROLLED_OVER'
            when c.end_date < p.as_of_date                             then 'EXPIRED'
            when datediff('day', p.as_of_date, c.end_date) <= 60       then 'RENEWAL_WINDOW'
            when datediff('month', c.start_date, p.as_of_date) < 3     then 'NEW'
            else 'IN_TERM'
        end                                                     as lifecycle_stage,
        case
            when c.end_date is null                                    then 'OPEN_ENDED'
            when datediff('month', p.as_of_date, c.end_date) <= 0      then 'ENDED'
            when datediff('month', p.as_of_date, c.end_date) <= 3      then '0-3M'
            when datediff('month', p.as_of_date, c.end_date) <= 12     then '4-12M'
            else '12M+'
        end                                                     as months_left_band,
        s.plan_id,
        pl.plan_name,
        pl.contract_type                                        as plan_contract_type,
        not equal_null(pl.contract_type, c.contract_type)       as is_plan_contract_mismatch,
        pl.internet_service                                     as plan_internet_service,
        pl.speed_mbps                                           as plan_speed_mbps,
        pl.data_cap_gb                                          as plan_data_cap_gb,
        pl.is_promo                                             as plan_is_promo,
        pl.feature_list,
        pl.feature_count,
        case
            when pl.base_price_php is null   then 'UNKNOWN'
            when pl.base_price_php < 500     then 'ENTRY'
            when pl.base_price_php < 1500    then 'MID'
            when pl.base_price_php < 3000    then 'PREMIUM'
            else 'TOP'
        end                                                     as price_band,
        coalesce(st.subscription_status, s.status)              as subscription_status,
        st.status_since,
        s.internet_service,
        s.has_phone_service,
        s.is_deleted                                            as is_subscription_deleted,
        a.payment_method,
        coalesce(a.payment_method ilike '%automatic%', false)   as is_auto_pay,
        a.is_paperless_billing,
        a.billing_cycle_day,
        a.is_senior,
        d.manufacturer                                          as device_manufacturer,
        d.device_type,
        d.supports_5g                                           as device_supports_5g,
        nvl2(s.partner_brand_id, pb.brand_name, 'OWN_BRAND')    as brand,
        coalesce(cs.region_name, 'UNKNOWN')                     as region_name,
        coalesce(sc.churn_band, 'UNSCORED')                     as churn_band,
        coalesce(a.is_test_customer, false)                     as is_test_customer
    from contracts c
    cross join params p
    join {{ ref('slv_subscription') }} s        on s.subscription_id = c.subscription_id
    left join status_as_of st                   on st.subscription_id = c.subscription_id
    left join {{ ref('slv_plan') }} pl          on pl.plan_id = s.plan_id
    left join account a                         on a.account_id = s.account_id
    left join {{ ref('slv_device') }} d         on d.device_id = s.device_id
    left join {{ ref('slv_partner_brand') }} pb on pb.partner_brand_id = s.partner_brand_id
    left join {{ ref('slv_cell_site') }} cs     on cs.cell_id = s.home_cell_id
    left join {{ ref('slv_churn_score') }} sc   on sc.subscription_id = c.subscription_id
)
select
    a.* exclude (is_test_customer),
    hash(a.contract_type, a.end_date, a.is_auto_renew, a.is_terminated, a.lifecycle_stage,
         a.plan_id, a.plan_name, a.price_band, a.feature_list, a.subscription_status,
         a.payment_method, a.is_paperless_billing, a.device_manufacturer, a.brand,
         a.region_name, a.churn_band)                           as hash_diff
from assembled a
where not a.is_test_customer
qualify row_number() over (partition by a.contract_id order by a.start_date desc) = 1
{% endsnapshot %}
