{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'marts', 'kpi'],
    transient=false,
    cluster_by=['billing_month'],
    copy_grants=true
) }}
-- Revenue assurance per account x billing month (replaces PIPE.SP_GOLD_REVENUE_RECON):
-- invoice header vs. invoice lines vs. earned revenue vs. cash vs. credits.
with inv as (
    select account_id, billing_month,
           count(*)                                         as invoices,
           sum(invoice_amount_php)                          as billed_php,
           sum(lines_php)                                   as lines_php,
           sum(paid_php)                                    as paid_php,
           sum(credits_php)                                 as credits_php,
           sum(outstanding_php)                             as outstanding_php
    from {{ ref('fct_invoice') }}
    where billing_month >= date_trunc('month', '{{ var("history_start_date") }}'::date)
    group by 1, 2
),
earned as (
    -- NOTE: in this dataset invoices do not reconcile to the earned-revenue feed under any stream
    -- definition (total, ex-RECHARGE, +VAT) - EARNED_MISMATCH is a genuine source-data finding.
    select p.account_id, m.month_start as billing_month, sum(m.total_revenue_php) as earned_php
    from {{ ref('fct_subscriber_monthly') }} m
    join {{ ref('int_subscriber_profile') }} p on p.subscription_id = m.subscription_id
    group by 1, 2
)
select
    i.account_id,
    i.billing_month,
    a.region_name,
    i.invoices,
    i.billed_php,
    i.lines_php,
    zeroifnull(e.earned_php)                                as earned_php,
    i.paid_php,
    i.credits_php,
    i.outstanding_php,
    round(i.billed_php - i.lines_php, 2)                    as header_line_gap_php,
    round(i.billed_php - zeroifnull(e.earned_php), 2)       as billed_earned_gap_php,
    case
        when abs(i.billed_php - i.lines_php) > 1                         then 'LINE_MISMATCH'
        when e.earned_php is not null
             and abs(i.billed_php - e.earned_php) > greatest(0.05 * i.billed_php, 5) then 'EARNED_MISMATCH'
        when i.outstanding_php > 0.005                                   then 'OPEN_BALANCE'
        else 'RECONCILED'
    end                                                     as recon_status
from inv i
left join earned e on e.account_id = i.account_id and e.billing_month = i.billing_month
left join (
    select account_id, any_value(home_region_name) as region_name
    from {{ ref('int_subscriber_profile') }} group by 1
) a on a.account_id = i.account_id
