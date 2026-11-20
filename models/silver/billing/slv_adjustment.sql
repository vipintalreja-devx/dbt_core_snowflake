{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'billing'],
    transient=true
) }}
-- Credits/adjustments. Bronze AMOUNT is negative; credit_amount_php is the positive credit value.
select
    adjustment_id,
    invoice_id,
    account_id,
    adjusted_at                                             as adjustment_date,
    amount::number(18, 2)                                   as adjustment_amount_php,
    abs(amount)::number(18, 2)                              as credit_amount_php,
    upper(reason_code)                                      as reason_code
from {{ source('billing', 'adjustment') }}
