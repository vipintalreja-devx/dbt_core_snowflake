{{ config(
    materialized='table',
    cluster_by=['assessment_date'],
    tags=['gold', 'analytics', 'billing', 'revenue_assurance', 'm20']
) }}
-- Late-fee revenue assurance: every assessed fee recomputed from the rule table (rate x invoice,
-- floored at the minimum fee, rule band by days overdue) and compared with what was charged.
-- Ported from x_m20_late_fee_recalc_daily (PIPE_X.SP_X_M20_LATE_FEE_RECALC_DAILY) - all assessment
-- dates instead of one AS_OF day.
-- Grain: invoice_id x assessment_date.
-- Sources: slv_late_fee_assessment, slv_invoice, source billing.late_fee_rule, fct_invoice, int_subscriber_profile.
with rules as (
    select rule_code, min_days, coalesce(max_days, 99999) as max_days, rate, min_fee_php
    from {{ source('billing', 'late_fee_rule') }}
),
expected_rule as (
    -- the rule the days-overdue SHOULD fall into
    select a.invoice_id, a.assessment_date, r.rule_code as expected_rule_code, r.rate as expected_rate, r.min_fee_php as expected_min_fee
    from {{ ref('slv_late_fee_assessment') }} a
    join rules r on a.days_overdue between r.min_days and r.max_days
    qualify row_number() over (partition by a.invoice_id, a.assessment_date order by r.min_days desc) = 1
),
acct_region as (
    select account_id, any_value(home_region_name) as region_name
    from {{ ref('int_subscriber_profile') }} group by 1
)
select
    a.invoice_id,
    a.assessment_date,
    a.account_id,
    ar.region_name,
    a.days_overdue,
    i.invoice_amount_php,
    fi.outstanding_php                                      as current_outstanding_php,
    a.rule_code                                             as charged_rule_code,
    er.expected_rule_code,
    a.fee_php                                               as charged_fee_php,
    round(iff(er.expected_rate = 0, 0, greatest(i.invoice_amount_php * er.expected_rate, er.expected_min_fee)), 2) as expected_fee_php,
    round(a.fee_php - iff(er.expected_rate = 0, 0, greatest(i.invoice_amount_php * er.expected_rate, er.expected_min_fee)), 2) as fee_variance_php,
    not equal_null(a.rule_code, er.expected_rule_code)      as is_wrong_rule,
    abs(a.fee_php - iff(er.expected_rate = 0, 0, greatest(i.invoice_amount_php * er.expected_rate, er.expected_min_fee))) > 0.01 as is_fee_mismatch,
    fi.is_settled                                           as invoice_now_settled,
    current_timestamp()                                     as loaded_at
from {{ ref('slv_late_fee_assessment') }} a
join {{ ref('slv_invoice') }} i      on i.invoice_id = a.invoice_id
left join {{ ref('fct_invoice') }} fi on fi.invoice_id = a.invoice_id
left join expected_rule er           on er.invoice_id = a.invoice_id and er.assessment_date = a.assessment_date
left join acct_region ar             on ar.account_id = a.account_id
