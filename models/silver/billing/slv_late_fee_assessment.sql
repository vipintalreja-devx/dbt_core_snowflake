{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'billing'],
    transient=true
) }}
select
    lfa.invoice_id,
    lfa.account_id,
    lfa.as_of                                               as assessment_date,
    lfa.rule_code,
    lfa.days_overdue,
    lfa.fee_php::number(18, 2)                              as fee_php,
    r.rate                                                  as rule_rate,
    r.min_fee_php                                           as rule_min_fee_php
from {{ source('billing', 'late_fee_assessment') }} lfa
left join {{ source('billing', 'late_fee_rule') }} r on r.rule_code = lfa.rule_code
