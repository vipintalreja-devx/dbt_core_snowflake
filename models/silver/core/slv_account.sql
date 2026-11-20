{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'core'],
    transient=true
) }}
select
    account_id,
    customer_id,
    nullif(trim(payment_method), '')                        as payment_method,
    coalesce(paperless_billing, false)                      as is_paperless_billing,
    billing_cycle_day,
    {{ local_ntz_to_utc('created_at') }}                    as created_at_utc,
    {{ utc_to_local(local_ntz_to_utc('created_at')) }}::date as created_date
from {{ source('core', 'account') }}
