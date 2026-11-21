{{ config(
    materialized='incremental',
    schema='silver',
    tags=['silver', 'billing', 'cdc'],
    transient=true,
    unique_key='invoice_id',
    incremental_strategy='merge',
    cluster_by=['billing_month'],
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('issue_date') }}",
               "{{ cdc_log_deletes('invoice_id', 'issue_date') }}",
               "{{ cdc_purge_tombstones() }}"]
) }}
-- Invoice headers, one row per INVOICE_ID (bronze contains a duplicated id).
-- CDC: new invoices from the bill run and daily status changes (UNPAID -> PAID / OVERDUE).
select
    invoice_id,
    account_id,
    source_customer_ref,
    billing_month,
    issue_date,
    due_date,
    total_amount::number(18, 2)                             as invoice_amount_php,
    currency_code,
    upper(invoice_status)                                   as invoice_status,
    _cdc_action,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ cdc_source('billing', 'invoice') }}
qualify row_number() over (partition by invoice_id order by iff(_cdc_action = 'DELETE', 1, 0) desc, issue_date desc, total_amount desc) = 1
