{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'facts', 'fact'],
    transient=false,
    cluster_by=['payment_date'],
    copy_grants=true
) }}
select
    p.payment_id, p.account_id, p.invoice_id, p.payment_date, p.paid_at_local,
    p.payment_channel, p.payment_method, p.payment_amount_php,
    i.due_date,
    datediff('day', i.due_date, p.payment_date)             as days_after_due,
    p.payment_date > i.due_date                             as is_late
from {{ ref('slv_payment') }} p
left join {{ ref('slv_invoice') }} i on i.invoice_id = p.invoice_id
