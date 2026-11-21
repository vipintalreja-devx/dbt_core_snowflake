{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='invoice_id',
    on_schema_change='append_new_columns',
    schema='gold',
    tags=['gold', 'facts', 'fact'],
    transient=false,
    cluster_by=['billing_month'],
    copy_grants=true
) }}
-- Grain: invoice. Billed vs. line items vs. payments vs. credits, with collection metrics.
-- Incremental: invoices whose header, lines, payments, credits or late fees changed since the
-- last run are recomputed over all their child rows (merge on invoice_id).
with changed as (
    select invoice_id from {{ cdc_changed_keys(ref('slv_invoice'), 'invoice_id') }}
    union select invoice_id from {{ cdc_changed_keys(ref('slv_invoice_line'), 'invoice_id') }}
    union select invoice_id from {{ cdc_changed_keys(ref('slv_payment'), 'invoice_id') }}
    union select invoice_id from {{ cdc_changed_keys(ref('slv_adjustment'), 'invoice_id') }}
    union select invoice_id from {{ cdc_changed_keys(ref('slv_late_fee_assessment'), 'invoice_id') }}
    {%- if is_incremental() %}
    -- payment reversals are hard deletes (row gone from silver): re-check every invoice
    -- that had a payment in the last 45 days
    union select invoice_id from {{ this }} where last_payment_date >= dateadd('day', -45, current_date())
    {%- endif %}
),
lines as (
    select invoice_id,
           sum(line_amount_php)                                     as lines_php,
           sum(iff(line_type = 'PLAN_FEE', line_amount_php, 0))     as plan_fee_php,
           sum(iff(line_type = 'ADDON',    line_amount_php, 0))     as addon_php,
           sum(iff(line_type = 'VAS',      line_amount_php, 0))     as vas_php,
           sum(iff(line_type = 'ROAMING',  line_amount_php, 0))     as roaming_php,
           sum(iff(line_type = 'TAX',      line_amount_php, 0))     as tax_php,
           sum(iff(line_type = 'DISCOUNT', line_amount_php, 0))     as discount_php,
           count(*)                                                 as line_count
    from {{ ref('slv_invoice_line') }}
    where invoice_id in (select invoice_id from changed)
    group by 1
),
pay as (
    select invoice_id,
           sum(payment_amount_php)                                  as paid_php,
           count(*)                                                 as payments,
           min(payment_date)                                        as first_payment_date,
           max(payment_date)                                        as last_payment_date
    from {{ ref('slv_payment') }}
    where invoice_id is not null
      and invoice_id in (select invoice_id from changed)
    group by 1
),
adj as (
    select invoice_id, sum(credit_amount_php) as credits_php, count(*) as adjustments
    from {{ ref('slv_adjustment') }}
    where invoice_id in (select invoice_id from changed)
    group by 1
),
fees as (
    select invoice_id, max(fee_php) as late_fee_php, max(days_overdue) as max_days_overdue
    from {{ ref('slv_late_fee_assessment') }}
    where invoice_id in (select invoice_id from changed)
    group by 1
)
select
    i.invoice_id,
    i.account_id,
    i.billing_month,
    i.issue_date,
    i.due_date,
    i.invoice_status,
    i.currency_code,
    i.invoice_amount_php,
    zeroifnull(l.lines_php)                                 as lines_php,
    zeroifnull(l.plan_fee_php)                              as plan_fee_php,
    zeroifnull(l.addon_php)                                 as addon_php,
    zeroifnull(l.vas_php)                                   as vas_php,
    zeroifnull(l.roaming_php)                               as roaming_php,
    zeroifnull(l.tax_php)                                   as tax_php,
    zeroifnull(l.discount_php)                              as discount_php,
    zeroifnull(l.line_count)                                as line_count,
    zeroifnull(p.paid_php)                                  as paid_php,
    zeroifnull(p.payments)                                  as payments,
    zeroifnull(a.credits_php)                               as credits_php,
    zeroifnull(f.late_fee_php)                              as late_fee_php,
    round(i.invoice_amount_php - zeroifnull(p.paid_php) - zeroifnull(a.credits_php), 2) as outstanding_php,
    round(i.invoice_amount_php - zeroifnull(l.lines_php), 2) as header_vs_lines_gap_php,
    p.first_payment_date,
    p.last_payment_date,
    datediff('day', i.issue_date, p.last_payment_date)      as days_to_pay,
    p.last_payment_date <= i.due_date                       as is_paid_on_time,
    i.invoice_amount_php - zeroifnull(p.paid_php) - zeroifnull(a.credits_php) <= 0.005 as is_settled
from {{ ref('slv_invoice') }} i
left join lines l on l.invoice_id = i.invoice_id
left join pay   p on p.invoice_id = i.invoice_id
left join adj   a on a.invoice_id = i.invoice_id
left join fees  f on f.invoice_id = i.invoice_id
where i.invoice_id in (select invoice_id from changed)
