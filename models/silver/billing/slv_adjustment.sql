{{ config(
    materialized='incremental',
    schema='silver',
    tags=['silver', 'billing', 'cdc'],
    transient=true,
    unique_key='adjustment_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('adjustment_date') }}",
               "{{ cdc_log_deletes('adjustment_id', 'adjustment_date') }}",
               "{{ cdc_purge_tombstones() }}"]
) }}
-- Credits/adjustments. Bronze AMOUNT is negative; credit_amount_php is the positive credit value. CDC from bronze.
select
    adjustment_id,
    invoice_id,
    account_id,
    adjusted_at                                             as adjustment_date,
    amount::number(18, 2)                                   as adjustment_amount_php,
    abs(amount)::number(18, 2)                              as credit_amount_php,
    upper(reason_code)                                      as reason_code,
    _cdc_action,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ cdc_source('billing', 'adjustment') }}
qualify row_number() over (partition by adjustment_id order by iff(_cdc_action = 'DELETE', 1, 0) desc) = 1
