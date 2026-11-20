{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'billing'],
    transient=true,
    cluster_by=['payment_date']
) }}
select
    payment_id,
    account_id,
    invoice_id,
    {{ tz_to_utc('paid_at') }}                              as paid_at_utc,
    {{ utc_to_local(tz_to_utc('paid_at')) }}                as paid_at_local,
    {{ utc_to_local(tz_to_utc('paid_at')) }}::date          as payment_date,
    amount::number(18, 2)                                   as payment_amount_php,
    case
        when payment_method ilike '%gcash%' or payment_method ilike '%maya%' then 'E-WALLET'
        when payment_method ilike '%credit card%'                            then 'CARD'
        when payment_method ilike '%bank%' or payment_method ilike '%electronic check%' then 'BANK'
        when payment_method ilike '%mailed check%'                           then 'CHECK'
        else 'UNKNOWN'
    end                                                     as payment_channel,
    nullif(payment_method, 'UNKNOWN')                       as payment_method
from {{ source('billing', 'payment') }}
