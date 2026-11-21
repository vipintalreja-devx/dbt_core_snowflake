{{ config(
    materialized='incremental',
    schema='silver',
    tags=['silver', 'billing', 'cdc'],
    transient=true,
    unique_key='dunning_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('action_date') }}",
               "{{ cdc_log_deletes('dunning_id', 'action_date') }}",
               "{{ cdc_purge_tombstones() }}"]
) }}
-- Collections (dunning) actions. CDC from bronze.
select
    dunning_id,
    account_id,
    invoice_id,
    dunning_stage,
    upper(action)                                           as dunning_action,
    {{ tz_to_utc('action_at') }}                            as action_at_utc,
    {{ utc_to_local(tz_to_utc('action_at')) }}::date        as action_date,
    amount_due::number(18, 2)                               as amount_due_php,
    _cdc_action,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ cdc_source('billing', 'dunning_event') }}
qualify row_number() over (partition by dunning_id order by iff(_cdc_action = 'DELETE', 1, 0) desc) = 1
