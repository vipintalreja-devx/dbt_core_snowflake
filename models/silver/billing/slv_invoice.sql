{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'billing'],
    transient=true,
    cluster_by=['billing_month']
) }}
-- Invoice headers, one row per INVOICE_ID (bronze contains a duplicated id).
select
    invoice_id,
    account_id,
    source_customer_ref,
    billing_month,
    issue_date,
    due_date,
    total_amount::number(18, 2)                             as invoice_amount_php,
    currency_code,
    upper(invoice_status)                                   as invoice_status
from {{ source('billing', 'invoice') }}
qualify row_number() over (partition by invoice_id order by issue_date desc, total_amount desc) = 1
