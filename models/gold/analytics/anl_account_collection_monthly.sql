{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='billing_month',
    on_schema_change='append_new_columns',
    cluster_by=['billing_month'],
    tags=['gold', 'analytics', 'billing', 'collections', 'm05']
) }}
-- Collections performance per account and billing month: billed vs. paid vs. credited, payment
-- behaviour (timeliness, channel), dunning escalation and a collection-risk segment.
-- Ported from x_m05_account_collection_monthly (PIPE_X.SP_X_M05_ACCOUNT_COLLECTION_MONTHLY).
-- Grain: account_id x billing_month.
-- Sources: fct_invoice, slv_payment, slv_dunning_event, slv_late_fee_assessment, dim_account, int_subscriber_profile.
with inv as (
    select *
    from {{ ref('fct_invoice') }}
    where {{ f2_month_window('billing_month', 'billing_month') }}
),
pay as (
    select
        i.account_id,
        i.billing_month,
        count(*)                                            as payment_count,
        mode(p.payment_channel)                             as main_payment_channel,
        max_by(p.payment_method, p.payment_amount_php)      as largest_payment_method,
        count_if(p.payment_date > i.due_date)               as late_payments,
        avg(datediff('day', i.issue_date, p.payment_date))  as avg_days_issue_to_pay
    from inv i
    join {{ ref('slv_payment') }} p on p.invoice_id = i.invoice_id
    group by 1, 2
),
dun as (
    select
        i.account_id,
        i.billing_month,
        max(d.dunning_stage)                                as max_dunning_stage,
        count(*)                                            as dunning_actions,
        boolor_agg(d.dunning_action = 'SERVICE_BARRED')     as was_barred,
        array_agg(distinct d.dunning_action) within group (order by d.dunning_action) as dunning_actions_list
    from inv i
    join {{ ref('slv_dunning_event') }} d on d.invoice_id = i.invoice_id
    group by 1, 2
),
fees as (
    select i.account_id, i.billing_month, sum(f.fee_php) as late_fees_php
    from inv i
    join {{ ref('slv_late_fee_assessment') }} f on f.invoice_id = i.invoice_id
    group by 1, 2
),
acct as (
    select
        a.account_id,
        a.payment_method                                    as account_payment_method,
        a.is_paperless_billing,
        a.active_lines,
        any_value(p.home_region_name)                       as region_name,
        any_value(p.island_group)                           as island_group
    from {{ ref('dim_account') }} a
    left join {{ ref('int_subscriber_profile') }} p on p.account_id = a.account_id
    group by 1, 2, 3, 4
),
inv_agg as (
    select
        account_id,
        billing_month,
        count(*)                                            as invoices,
        sum(invoice_amount_php)                             as billed_php,
        sum(paid_php)                                       as paid_php,
        sum(credits_php)                                    as credited_php,
        sum(outstanding_php)                                as balance_php,
        count_if(is_paid_on_time)                           as invoices_paid_on_time,
        max(days_to_pay)                                    as max_days_to_pay
    from inv
    group by 1, 2
)
select
    ia.account_id,
    ia.billing_month,
    ac.region_name,
    ac.island_group,
    ac.account_payment_method,
    ac.is_paperless_billing,
    ac.active_lines,
    ia.invoices,
    round(ia.billed_php, 2)                                 as billed_php,
    round(ia.paid_php, 2)                                   as paid_php,
    round(ia.credited_php, 2)                               as credited_php,
    round(ia.balance_php, 2)                                as balance_php,
    round(zeroifnull(fe.late_fees_php), 2)                  as late_fees_php,
    round(div0(ia.paid_php, ia.billed_php), 4)              as collection_rate,
    ia.invoices_paid_on_time,
    ia.max_days_to_pay,
    zeroifnull(py.payment_count)                            as payment_count,
    zeroifnull(py.late_payments)                            as late_payments,
    round(py.avg_days_issue_to_pay, 1)                      as avg_days_issue_to_pay,
    py.main_payment_channel,
    py.largest_payment_method,
    zeroifnull(dn.max_dunning_stage)                        as max_dunning_stage,
    zeroifnull(dn.dunning_actions)                          as dunning_actions,
    coalesce(dn.was_barred, false)                          as was_barred,
    dn.dunning_actions_list,
    case
        when coalesce(dn.was_barred, false)                                         then 'BARRED'
        when ia.balance_php > 0.005 and zeroifnull(dn.max_dunning_stage) >= 2       then 'HIGH_RISK'
        when ia.balance_php > 0.005                                                 then 'OPEN'
        when zeroifnull(py.late_payments) > 0                                       then 'PAID_LATE'
        else 'GOOD_STANDING'
    end                                                     as collection_segment,
    current_timestamp()                                     as loaded_at
from inv_agg ia
left join pay  py on py.account_id = ia.account_id and py.billing_month = ia.billing_month
left join dun  dn on dn.account_id = ia.account_id and dn.billing_month = ia.billing_month
left join fees fe on fe.account_id = ia.account_id and fe.billing_month = ia.billing_month
left join acct ac on ac.account_id = ia.account_id
