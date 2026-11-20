{{ config(
    materialized='table',
    cluster_by=['billing_month'],
    tags=['gold', 'aggregates', 'billing', 'e03']
) }}
-- Invoices per billing month x status x home region, with collection performance.
-- Ported from x_e03_invoice_status_monthly (PIPE_X.SP_X_E03_INVOICE_STATUS_MONTHLY); full history,
-- one statement, instead of one CALL per month.
-- Grain   : billing_month x invoice_status x region_name
-- Sources : fct_invoice, dim_account, int_subscriber_profile, dim_region
with acct_region as (
    select account_id,
           max_by(home_region_id, monthly_charges_php)      as region_id
    from {{ ref('int_subscriber_profile') }}
    group by 1
),
inv as (
    select
        f.invoice_id, f.account_id, f.billing_month, f.invoice_status,
        f.invoice_amount_php, f.paid_php, f.credits_php, f.outstanding_php, f.late_fee_php,
        f.days_to_pay, f.is_paid_on_time, f.is_settled,
        a.is_paperless_billing,
        coalesce(r.region_name, 'UNKNOWN')                  as region_name,
        coalesce(r.island_group, 'UNKNOWN')                 as island_group
    from {{ ref('fct_invoice') }} f
    left join {{ ref('dim_account') }} a on a.account_id = f.account_id
    left join acct_region ar            on ar.account_id = f.account_id
    left join {{ ref('dim_region') }} r on r.region_id = ar.region_id
),
monthly as (
    select
        billing_month, invoice_status, region_name,
        any_value(island_group)                             as island_group,
        count(*)                                            as invoices,
        approx_count_distinct(account_id)                   as accounts_approx,
        sum(invoice_amount_php)                             as billed_php,
        sum(paid_php)                                       as paid_php,
        sum(credits_php)                                    as credits_php,
        sum(outstanding_php)                                as outstanding_php,
        sum(late_fee_php)                                   as late_fee_php,
        round(avg(invoice_amount_php), 2)                   as avg_invoice_php,
        approx_percentile(invoice_amount_php, 0.9)          as p90_invoice_php,
        median(days_to_pay)                                 as median_days_to_pay,
        count_if(is_paid_on_time)                           as paid_on_time,
        count_if(is_settled)                                as settled,
        count_if(is_paperless_billing)                      as paperless_invoices
    from inv
    group by 1, 2, 3
)
select
    m.*,
    round(div0(m.paid_php + m.credits_php, m.billed_php) * 100, 2)       as collection_rate_pct,
    round(div0(m.paperless_invoices, m.invoices) * 100, 2)               as paperless_pct,
    round(ratio_to_report(m.billed_php) over (partition by m.billing_month, m.region_name), 6) as status_share_of_region_billed,
    lag(m.billed_php) over (partition by m.invoice_status, m.region_name order by m.billing_month) as prev_month_billed_php,
    round(div0(m.billed_php - lag(m.billed_php) over (partition by m.invoice_status, m.region_name order by m.billing_month),
               lag(m.billed_php) over (partition by m.invoice_status, m.region_name order by m.billing_month)) * 100, 2) as billed_mom_pct,
    current_timestamp()                                     as loaded_at
from monthly m
