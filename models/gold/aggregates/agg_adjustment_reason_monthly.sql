{{ config(
    materialized='table',
    cluster_by=['adjustment_month'],
    tags=['gold', 'aggregates', 'billing', 'revenue_assurance', 'e04']
) }}
-- Credits/adjustments per month x reason x region, sized against the invoices they adjust.
-- Ported from x_e04_adjustment_reason_monthly (PIPE_X.SP_X_E04_ADJUSTMENT_REASON_MONTHLY).
-- Grain   : adjustment_month x reason_code x region_name
-- Sources : slv_adjustment, slv_invoice, int_subscriber_profile, dim_region, fct_outage (outage credits context)
with acct_region as (
    select account_id, max_by(home_region_id, monthly_charges_php) as region_id
    from {{ ref('int_subscriber_profile') }}
    group by 1
),
adj as (
    select
        date_trunc('month', a.adjustment_date)::date        as adjustment_month,
        a.reason_code,
        a.adjustment_id, a.account_id, a.credit_amount_php,
        i.invoice_amount_php,
        i.billing_month,
        datediff('day', i.issue_date, a.adjustment_date)    as days_after_issue,
        coalesce(r.region_name, 'UNKNOWN')                  as region_name,
        r.region_id
    from {{ ref('slv_adjustment') }} a
    left join {{ ref('slv_invoice') }} i on i.invoice_id = a.invoice_id
    left join acct_region ar            on ar.account_id = a.account_id
    left join {{ ref('dim_region') }} r on r.region_id = ar.region_id
),
outage_ctx as (
    select date_trunc('month', event_date)::date as adjustment_month, region_id,
           sum(outage_minutes) as outage_minutes, sum(credit_eligible_lines) as credit_eligible_lines
    from {{ ref('fct_outage') }}
    group by 1, 2
),
monthly as (
    select
        adjustment_month, reason_code, region_name,
        any_value(region_id)                                as region_id,
        count(*)                                            as adjustments,
        approx_count_distinct(account_id)                   as accounts_approx,
        sum(credit_amount_php)                              as credit_php,
        min(credit_amount_php)                              as min_credit_php,
        max(credit_amount_php)                              as max_credit_php,
        median(credit_amount_php)                           as median_credit_php,
        round(avg(div0(credit_amount_php, invoice_amount_php)) * 100, 2) as avg_credit_pct_of_invoice,
        round(avg(days_after_issue), 1)                     as avg_days_after_issue,
        count_if(credit_amount_php >= invoice_amount_php)   as full_invoice_credits
    from adj
    group by 1, 2, 3
)
select
    m.adjustment_month, m.reason_code, m.region_name,
    m.adjustments, m.accounts_approx, m.credit_php, m.min_credit_php, m.max_credit_php, m.median_credit_php,
    m.avg_credit_pct_of_invoice, m.avg_days_after_issue, m.full_invoice_credits,
    iff(m.reason_code = 'OUTAGE_CREDIT', o.outage_minutes, null)         as region_outage_minutes,
    iff(m.reason_code = 'OUTAGE_CREDIT', o.credit_eligible_lines, null)  as region_credit_eligible_lines,
    round(ratio_to_report(m.credit_php) over (partition by m.adjustment_month), 6)          as share_of_month_credit,
    rank() over (partition by m.adjustment_month, m.region_name order by m.credit_php desc)  as reason_rank_in_region,
    round(div0(m.credit_php - lag(m.credit_php) over (partition by m.reason_code, m.region_name order by m.adjustment_month),
               lag(m.credit_php) over (partition by m.reason_code, m.region_name order by m.adjustment_month)) * 100, 2) as credit_mom_pct,
    current_timestamp()                                     as loaded_at
from monthly m
left join outage_ctx o on o.adjustment_month = m.adjustment_month and o.region_id = m.region_id
