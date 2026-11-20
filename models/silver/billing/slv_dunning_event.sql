{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'billing'],
    transient=true
) }}
select
    dunning_id,
    account_id,
    invoice_id,
    dunning_stage,
    upper(action)                                           as dunning_action,
    {{ tz_to_utc('action_at') }}                            as action_at_utc,
    {{ utc_to_local(tz_to_utc('action_at')) }}::date        as action_date,
    amount_due::number(18, 2)                               as amount_due_php
from {{ source('billing', 'dunning_event') }}
