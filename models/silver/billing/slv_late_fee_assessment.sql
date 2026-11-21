{%- set changed = cdc_source_changed_dates('billing', 'late_fee_assessment', 'as_of') -%}
{{ config(
    materialized='incremental',
    schema='silver',
    tags=['silver', 'billing', 'cdc'],
    transient=true,
    unique_key='assessment_date',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('assessment_date', has_action=false) }}"]
) }}
-- Month-end late-fee assessments. CDC: assessment dates with changed rows are recomputed.
select
    lfa.invoice_id,
    lfa.account_id,
    lfa.as_of                                               as assessment_date,
    lfa.rule_code,
    lfa.days_overdue,
    lfa.fee_php::number(18, 2)                              as fee_php,
    r.rate                                                  as rule_rate,
    r.min_fee_php                                           as rule_min_fee_php,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ source('billing', 'late_fee_assessment') }} lfa
left join {{ source('billing', 'late_fee_rule') }} r on r.rule_code = lfa.rule_code
{% if changed %}where lfa.as_of in {{ changed }}{% endif %}
