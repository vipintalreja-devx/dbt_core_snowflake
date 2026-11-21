{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='assessment_date',
    on_schema_change='append_new_columns',
    cluster_by=['assessment_date'],
    tags=['gold', 'aggregates', 'billing', 'e06']
) }}
-- Late fees assessed per day x rule x region, with the fee the rule table implies (revenue assurance).
-- Ported from x_e06_late_fee_daily (PIPE_X.SP_X_E06_LATE_FEE_DAILY).
-- Grain   : assessment_date x rule_code x region_name
-- Sources : slv_late_fee_assessment, slv_invoice, int_subscriber_profile, dim_region
with acct_region as (
    select account_id, max_by(home_region_id, monthly_charges_php) as region_id
    from {{ ref('int_subscriber_profile') }}
    group by 1
),
fees as (
    select
        l.assessment_date, l.rule_code, l.invoice_id, l.account_id, l.days_overdue, l.fee_php,
        i.invoice_amount_php,
        iff(l.rule_rate = 0, 0, greatest(round(i.invoice_amount_php * l.rule_rate, 2), coalesce(l.rule_min_fee_php, 0))) as expected_fee_php,
        coalesce(r.region_name, 'UNKNOWN')                  as region_name
    from {{ ref('slv_late_fee_assessment') }} l
    left join {{ ref('slv_invoice') }} i on i.invoice_id = l.invoice_id
    left join acct_region ar            on ar.account_id = l.account_id
    left join {{ ref('dim_region') }} r on r.region_id = ar.region_id
    where l.assessment_date >= {{ f1_lower_bound('assessment_date') }}
)
select
    assessment_date, rule_code, region_name,
    count(*)                                                as invoices,
    approx_count_distinct(account_id)                       as accounts_approx,
    sum(fee_php)                                            as fee_php,
    sum(expected_fee_php)                                   as expected_fee_php,
    round(sum(fee_php) - sum(expected_fee_php), 2)          as fee_variance_php,
    count_if(abs(fee_php - expected_fee_php) > 0.01)        as invoices_with_fee_variance,
    round(avg(days_overdue), 1)                             as avg_days_overdue,
    max(days_overdue)                                       as max_days_overdue,
    round(div0(sum(fee_php), sum(invoice_amount_php)) * 100, 3) as fee_pct_of_billed,
    round(ratio_to_report(sum(fee_php)) over (partition by assessment_date), 6) as share_of_day_fee,
    current_timestamp()                                     as loaded_at
from fees
group by 1, 2, 3
