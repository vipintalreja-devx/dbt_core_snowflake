{{ config(
    materialized='incremental',
    schema='silver',
    tags=['silver', 'billing', 'cdc'],
    transient=true,
    unique_key='payment_id',
    incremental_strategy='merge',
    cluster_by=['payment_date'],
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('payment_date') }}",
               "{{ cdc_log_deletes('payment_id', 'payment_date') }}",
               "{{ cdc_purge_tombstones() }}"]
) }}
-- Payments (gateway files via Snowpipe). CDC: new payments, allocation updates
-- (invoice_id filled at bill run) and reversals (hard deletes) since the last run.
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
    nullif(payment_method, 'UNKNOWN')                       as payment_method,
    _cdc_action,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ cdc_source('billing', 'payment') }}
qualify row_number() over (partition by payment_id order by iff(_cdc_action = 'DELETE', 1, 0) desc, paid_at desc) = 1
