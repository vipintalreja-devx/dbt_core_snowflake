{{ config(
    materialized='incremental',
    schema='silver',
    tags=['silver', 'billing', 'cdc'],
    transient=true,
    unique_key=['invoice_id', 'line_no'],
    incremental_strategy='merge',
    cluster_by=['invoice_id'],
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_purge_tombstones() }}"]
) }}
-- Invoice lines. CDC from bronze; grain invoice x line.
select
    invoice_id,
    line_no,
    upper(line_type)                                        as line_type,
    description,
    amount::number(18, 2)                                   as line_amount_php,
    amount_precise                                          as line_amount_precise_php,
    _cdc_action,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ cdc_source('billing', 'invoice_line') }}
qualify row_number() over (partition by invoice_id, line_no order by iff(_cdc_action = 'DELETE', 1, 0) desc, amount_precise desc) = 1
