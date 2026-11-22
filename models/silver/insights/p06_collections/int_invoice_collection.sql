{{ config(
    materialized='incremental',
    schema='silver',
    tags=['insights', 'p06', 'billing', 'collections'],
    incremental_strategy='delete+insert',
    unique_key='billing_month',
    cluster_by=['billing_month'],
    on_schema_change='append_new_columns'
) }}
-- P06 Collections: the collection state of every invoice.
-- Purpose : payments, credits, late fees and dunning per invoice; the day the invoice was fully
--           settled (first day cumulative payments reach the amount net of credits); days to
--           pay and days past due; collection state. Payments keep arriving for ~3 months after
--           issue, so each run restates the last three billing months.
-- Grain   : invoice_id
-- Sources : slv_invoice, slv_payment, slv_adjustment, slv_late_fee_assessment, slv_dunning_event, int_subscriber_profile
{%- set window_start = "dateadd('month', -2, " ~ f3_month_lower_bound('billing_month') ~ ")" %}
with inv as (
    select invoice_id, account_id, billing_month, issue_date, due_date, invoice_amount_php, invoice_status
    from {{ ref('slv_invoice') }}
    where billing_month >= {{ window_start }}
      and account_id is not null
),
pay_daily as (
    select p.invoice_id, p.payment_date, sum(p.payment_amount_php) as paid_php, count(*) as payments
    from {{ ref('slv_payment') }} p
    where p.payment_date >= dateadd('month', -1, {{ window_start }})
      and p.invoice_id in (select invoice_id from inv)
    group by 1, 2
),
credits as (
    select invoice_id, sum(credit_amount_php) as credits_php, count(*) as adjustments
    from {{ ref('slv_adjustment') }}
    where adjustment_date >= dateadd('month', -1, {{ window_start }})
      and invoice_id in (select invoice_id from inv)
    group by 1
),
fees as (
    select invoice_id, max(fee_php) as late_fee_php, max(days_overdue) as max_days_overdue_assessed
    from {{ ref('slv_late_fee_assessment') }}
    where invoice_id in (select invoice_id from inv)
    group by 1
),
dunning as (
    select invoice_id, count(*) as dunning_actions, max(dunning_stage) as max_dunning_stage,
           min(action_date) as first_dunning_date, max(action_date) as last_dunning_date
    from {{ ref('slv_dunning_event') }}
    where action_date >= {{ window_start }}
      and invoice_id in (select invoice_id from inv)
    group by 1
),
pay_running as (
    select pd.invoice_id, pd.payment_date, pd.paid_php,
           sum(pd.paid_php) over (partition by pd.invoice_id order by pd.payment_date
                                  rows between unbounded preceding and current row) as paid_to_date_php
    from pay_daily pd
),
pay_summary as (
    select r.invoice_id,
           sum(r.paid_php)                                                  as paid_php,
           min(r.payment_date)                                              as first_payment_date,
           max(r.payment_date)                                              as last_payment_date,
           min(iff(r.paid_to_date_php >= i.invoice_amount_php - zeroifnull(c.credits_php) - 0.005, r.payment_date, null)) as settled_date
    from pay_running r
    join inv i on i.invoice_id = r.invoice_id
    left join credits c on c.invoice_id = r.invoice_id
    group by 1
),
account_region as (
    select account_id, max_by(home_region_id, activation_date) as region_id
    from {{ ref('int_subscriber_profile') }}
    group by 1
),
as_of as (
    select max(payment_date) as as_of_date from pay_daily
)
select
    i.invoice_id, i.account_id, ar.region_id, i.billing_month, i.issue_date, i.due_date,
    i.invoice_amount_php, i.invoice_status,
    zeroifnull(ps.paid_php)                                                 as paid_php,
    zeroifnull(c.credits_php)                                               as credits_php,
    zeroifnull(f.late_fee_php)                                              as late_fee_php,
    greatest(i.invoice_amount_php - zeroifnull(ps.paid_php) - zeroifnull(c.credits_php), 0) as outstanding_php,
    ps.first_payment_date, ps.last_payment_date, ps.settled_date,
    datediff('day', i.issue_date, ps.settled_date)                          as days_to_settle,
    greatest(datediff('day', i.due_date, ps.settled_date), 0)               as days_past_due_at_settle,
    zeroifnull(d.dunning_actions)                                           as dunning_actions,
    zeroifnull(d.max_dunning_stage)                                         as max_dunning_stage,
    d.first_dunning_date, d.last_dunning_date,
    case when ps.settled_date is not null and ps.settled_date <= i.due_date then 'PAID_ON_TIME'
         when ps.settled_date is not null then 'PAID_LATE'
         when i.due_date >= a.as_of_date then 'OPEN_NOT_DUE'
         else 'OVERDUE_OPEN' end                                            as collection_state,
    a.as_of_date,
    {{ dbt_loaded_at() }}                                                   as _dbt_loaded_at
from inv i
cross join as_of a
left join pay_summary ps on ps.invoice_id = i.invoice_id
left join credits c on c.invoice_id = i.invoice_id
left join fees f on f.invoice_id = i.invoice_id
left join dunning d on d.invoice_id = i.invoice_id
left join account_region ar on ar.account_id = i.account_id
