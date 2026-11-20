{% snapshot snap_account_profile %}
{{ config(
    unique_key='account_id',
    strategy='check',
    check_cols=['hash_diff'],
    invalidate_hard_deletes=True,
    tags=['snapshot', 'scd2', 'account', 'h04']
) }}
{#-
  Account billing & relationship profile, SCD Type 2 (check strategy on HASH_DIFF of BANDED attributes,
  so a small change in spend does not create a version).
  Ported from telco_v2_core snapshot x_h04_dim_account_profile_scd2 (PIPE_X.SP_X_H04).
  Grain: one row per account (current state).
  Sources (12): slv_account, slv_customer, slv_customer_consent, slv_subscription,
  slv_segment_membership, slv_contract, slv_service_instance, slv_invoice, slv_payment,
  slv_daily_revenue, slv_plan, slv_subscription_status_history.
-#}
with params as (
    select max(valid_from_date) as as_of_date from {{ ref('slv_subscription_status_history') }}
),
consents as (
    select
        customer_id,
        array_agg(distinct iff(is_granted, consent_type, null)) within group (order by iff(is_granted, consent_type, null)) as granted_consents,
        array_union_agg(iff(is_granted, channel_list, array_construct()))                  as consent_channels
    from {{ ref('slv_customer_consent') }}
    group by customer_id
),
lines as (
    select
        s.account_id,
        count(*)                                                as total_lines,
        count_if(s.status in ('ACTIVE', 'SUSPENDED'))           as active_lines,
        count_if(s.partner_brand_id is not null)                as mvno_lines,
        mode(s.plan_id)                                         as main_plan_id,
        array_agg(distinct s.plan_id) within group (order by s.plan_id) as plan_ids
    from {{ ref('slv_subscription') }} s
    where not s.is_deleted
    group by s.account_id
),
segments as (
    select s.account_id, mode(sm.segment_code) as main_segment
    from {{ ref('slv_segment_membership') }} sm
    join {{ ref('slv_subscription') }} s on s.subscription_id = sm.subscription_id
    where sm.is_current
    group by s.account_id
),
contracts as (
    select s.account_id, mode(c.contract_type) as main_contract_type,
           max(c.end_date) as latest_contract_end, count_if(c.is_auto_renew) as auto_renew_contracts
    from {{ ref('slv_contract') }} c
    join {{ ref('slv_subscription') }} s on s.subscription_id = c.subscription_id
    where not c.is_terminated
    group by s.account_id
),
fixed as (
    select s.account_id, count(*) as fixed_services, count_if(si.service_type = 'FTTH') as ftth_services
    from {{ ref('slv_service_instance') }} si
    join {{ ref('slv_subscription') }} s on s.subscription_id = si.subscription_id
    where si.is_active
    group by s.account_id
),
last_invoices as (
    -- payment discipline from the last six invoices
    select i.account_id, i.invoice_id, i.due_date, i.invoice_amount_php,
           max(p.payment_date) as last_payment_date, sum(p.payment_amount_php) as paid_php
    from {{ ref('slv_invoice') }} i
    left join {{ ref('slv_payment') }} p on p.invoice_id = i.invoice_id
    group by 1, 2, 3, 4
    qualify row_number() over (partition by i.account_id order by i.due_date desc) <= 6
),
discipline as (
    select account_id,
           count(*)                                             as invoices_6,
           count_if(last_payment_date <= due_date and paid_php >= invoice_amount_php - 0.005) as on_time_6,
           count_if(paid_php is null)                           as unpaid_6,
           avg(datediff('day', due_date, last_payment_date))    as avg_days_vs_due
    from last_invoices
    group by account_id
),
value as (
    select s.account_id, sum(r.revenue_php) / greatest(count(distinct date_trunc('month', r.business_date)), 1) as avg_monthly_revenue_php
    from {{ ref('slv_daily_revenue') }} r
    join {{ ref('slv_subscription') }} s on s.subscription_id = r.subscription_id
    cross join params p
    where r.business_date > dateadd('month', -3, p.as_of_date)
    group by s.account_id
),
profile as (
    select
        a.account_id,
        a.customer_id,
        cu.customer_code,
        a.payment_method,
        a.is_paperless_billing,
        a.billing_cycle_day,
        lower(cu.preferred_language)                            as preferred_language,
        case
            when cu.kyc_verified_on is null                                 then 'NONE'
            when cu.kyc_has_address_proof and cu.kyc_liveness_score >= 0.8  then 'FULL'
            when cu.kyc_has_address_proof                                   then 'STANDARD'
            else 'BASIC'
        end                                                     as kyc_level,
        array_to_string(cn.granted_consents, ',')               as granted_consents,
        array_size(cn.consent_channels)                         as consent_channel_count,
        zeroifnull(l.total_lines)                               as total_lines,
        zeroifnull(l.active_lines)                              as active_lines,
        zeroifnull(l.mvno_lines)                                as mvno_lines,
        zeroifnull(f.fixed_services)                            as fixed_services,
        zeroifnull(f.ftth_services)                             as ftth_services,
        case
            when zeroifnull(l.active_lines) = 0                             then 'INACTIVE'
            when zeroifnull(l.active_lines) + zeroifnull(f.fixed_services) >= 4 then 'FAMILY'
            when zeroifnull(l.active_lines) + zeroifnull(f.fixed_services) >= 2 then 'MULTI'
            else 'SINGLE'
        end                                                     as household_band,
        l.main_plan_id,
        pl.plan_name                                            as main_plan_name,
        coalesce(sg.main_segment, 'NONE')                       as main_segment,
        ct.main_contract_type,
        ct.latest_contract_end,
        d.invoices_6,
        d.on_time_6,
        round(d.avg_days_vs_due, 1)                             as avg_days_vs_due,
        case
            when d.invoices_6 is null                           then 'NO_HISTORY'
            when div0(d.on_time_6, d.invoices_6) >= 0.9         then 'EXCELLENT'
            when div0(d.on_time_6, d.invoices_6) >= 0.6         then 'GOOD'
            when d.unpaid_6 >= 3                                then 'DELINQUENT'
            else 'IRREGULAR'
        end                                                     as payment_discipline,
        round(v.avg_monthly_revenue_php, 2)                     as avg_monthly_revenue_php,
        case
            when v.avg_monthly_revenue_php is null              then 'NO_REVENUE'
            when v.avg_monthly_revenue_php >= 3000              then 'HIGH'
            when v.avg_monthly_revenue_php >= 1000              then 'MEDIUM'
            else 'LOW'
        end                                                     as value_band
    from {{ ref('slv_account') }} a
    join {{ ref('slv_customer') }} cu   on cu.customer_id = a.customer_id
    left join consents cn               on cn.customer_id = a.customer_id
    left join lines l                   on l.account_id = a.account_id
    left join fixed f                   on f.account_id = a.account_id
    left join segments sg               on sg.account_id = a.account_id
    left join contracts ct              on ct.account_id = a.account_id
    left join discipline d              on d.account_id = a.account_id
    left join value v                   on v.account_id = a.account_id
    left join {{ ref('slv_plan') }} pl  on pl.plan_id = l.main_plan_id
    where not cu.is_test_customer
)
select
    pr.*,
    decode(pr.preferred_language, 'en', 'English', 'fil', 'Filipino', 'ceb', 'Cebuano', 'ilo', 'Ilocano', 'Other') as language_name,
    hash(pr.payment_method, pr.is_paperless_billing, pr.billing_cycle_day, pr.preferred_language, pr.kyc_level,
         pr.granted_consents, pr.household_band, pr.main_plan_id, pr.main_segment, pr.main_contract_type,
         pr.payment_discipline, pr.value_band)                  as hash_diff
from profile pr
{% endsnapshot %}
