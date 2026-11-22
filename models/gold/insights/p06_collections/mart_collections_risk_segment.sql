{{ config(
    materialized='table',
    schema='gold',
    tags=['insights', 'p06', 'billing', 'collections', 'mart'],
    transient=false,
    cluster_by=['region_id', 'risk_segment'],
    copy_grants=true
) }}
-- P06 Collections: payment-risk segment of every billed account (current state).
-- Purpose : behaviour over the last six billing months -> a 0-100 collection risk score and a
--           LOW / MEDIUM / HIGH / CRITICAL segment, with the account's percentile in its region.
--   score = 35 x late-payment ratio + 10 x highest dunning stage reached (0-3)
--         + 15 if any balance is overdue + 10 x min(avg days past due, 30) / 30
--         + 10 if service was ever barred
-- Grain   : account_id
-- Sources : int_invoice_collection
with recent as (
    select *
    from {{ ref('int_invoice_collection') }}
    where billing_month >= dateadd('month', -5, (select max(billing_month) from {{ ref('int_invoice_collection') }}))
),
features as (
    select
        account_id,
        max_by(region_id, billing_month)                                    as region_id,
        count(*)                                                            as invoices_6m,
        count_if(collection_state = 'PAID_ON_TIME')                         as paid_on_time_6m,
        count_if(collection_state in ('PAID_LATE', 'OVERDUE_OPEN'))         as paid_late_or_open_6m,
        round(div0(count_if(collection_state in ('PAID_LATE', 'OVERDUE_OPEN')),
                   count_if(collection_state <> 'OPEN_NOT_DUE')), 3)        as late_ratio,
        round(avg(iff(collection_state = 'PAID_LATE', days_past_due_at_settle, null)), 1) as avg_days_past_due,
        max(max_dunning_stage)                                              as max_dunning_stage,
        count_if(max_dunning_stage = 3)                                     as barred_invoices,
        round(sum(iff(collection_state = 'OVERDUE_OPEN', outstanding_php, 0)), 2) as overdue_balance_php,
        round(sum(late_fee_php), 2)                                         as late_fees_6m_php,
        round(sum(invoice_amount_php), 2)                                   as billed_6m_php
    from recent
    group by account_id
),
scored as (
    select f.*,
           round(least(100,
                 35 * zeroifnull(late_ratio)
               + 10 * zeroifnull(max_dunning_stage)
               + iff(overdue_balance_php > 0, 15, 0)
               + 10 * least(zeroifnull(avg_days_past_due), 30) / 30
               + iff(barred_invoices > 0, 10, 0)), 1)                       as risk_score
    from features f
)
select
    s.*,
    case when risk_score >= 70 then 'CRITICAL'
         when risk_score >= 45 then 'HIGH'
         when risk_score >= 20 then 'MEDIUM'
         else 'LOW' end                                                     as risk_segment,
    round(percent_rank() over (partition by region_id order by risk_score) * 100, 1) as risk_percentile_in_region,
    (select max(as_of_date) from {{ ref('int_invoice_collection') }})       as as_of_date
from scored s
