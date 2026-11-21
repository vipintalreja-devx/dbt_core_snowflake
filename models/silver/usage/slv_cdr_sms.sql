{{ config(
    tags=['silver', 'usage', 'cdc'],
    schema='silver',
    materialized='incremental',
    unique_key='sms_id',
    incremental_strategy='merge',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns',
    full_refresh=false,
    post_hook=["{{ cdc_log_changed_dates('event_date') }}",
               "{{ cdc_log_deletes('sms_id', 'event_date') }}",
               "{{ cdc_purge_tombstones() }}"]
) }}
-- SMS CDRs. CDC from bronze (see cdc.sql).
with src as (
    select *
    from {{ cdc_source('usage', 'cdr_sms', "sent_at >= " ~ incremental_ts_lower_bound() ~ "::timestamp_tz") }}
    qualify row_number() over (partition by sms_id order by iff(_cdc_action = 'DELETE', 1, 0) desc, ingested_at desc nulls last) = 1
),
shaped as (
    select
        sms_id,
        subscription_id,
        {{ normalize_msisdn('sender_msisdn') }}             as sender_msisdn,
        {{ normalize_msisdn('recipient_msisdn') }}          as recipient_msisdn,
        cell_id,
        {{ tz_to_utc('sent_at') }}                          as sent_at_utc,
        {{ utc_to_local(tz_to_utc('sent_at')) }}            as sent_at_local,
        coalesce(is_roaming, false)                         as is_roaming,
        upper(sms_type)                                     as sms_type,
        charge_php::number(18, 4)                           as charge_php,
        {{ tz_to_utc('ingested_at') }}                      as ingested_at_utc,
        _cdc_action
    from src
    where subscription_id is not null
)
select * exclude (_cdc_action), sent_at_local::date as event_date, _cdc_action, {{ dbt_loaded_at() }} as _dbt_loaded_at
from shaped
where {{ full_refresh_floor('sent_at_local::date') }}
