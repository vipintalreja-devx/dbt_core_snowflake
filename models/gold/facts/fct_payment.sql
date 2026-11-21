{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='payment_id',
    on_schema_change='append_new_columns',
    schema='gold',
    tags=['gold', 'facts', 'fact'],
    transient=false,
    cluster_by=['payment_date'],
    copy_grants=true,
    post_hook=["{{ cdc_apply_upstream_deletes('payment_id', 'slv_payment') }}"]
) }}
-- Incremental: payments changed in silver (new, allocated, reversed) plus payments of
-- invoices whose due date changed are re-merged; reversals are deleted by the post-hook.
with changed as (
    select payment_id from {{ cdc_changed_keys(ref('slv_payment'), 'payment_id') }}
    union
    select p.payment_id from {{ ref('slv_payment') }} p
    where p.invoice_id in (select invoice_id from {{ cdc_changed_keys(ref('slv_invoice'), 'invoice_id') }})
)
select
    p.payment_id, p.account_id, p.invoice_id, p.payment_date, p.paid_at_local,
    p.payment_channel, p.payment_method, p.payment_amount_php,
    i.due_date,
    datediff('day', i.due_date, p.payment_date)             as days_after_due,
    p.payment_date > i.due_date                             as is_late
from {{ ref('slv_payment') }} p
left join {{ ref('slv_invoice') }} i on i.invoice_id = p.invoice_id
where p.payment_id in (select payment_id from changed)
