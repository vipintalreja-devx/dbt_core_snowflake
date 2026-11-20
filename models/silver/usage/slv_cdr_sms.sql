{{ config(
    tags=['silver', 'usage'],
    schema='silver',
    materialized='incremental',
    unique_key='sms_id',
    incremental_strategy='merge',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns'
) }}
with src as (
    select *
    from {{ source('usage', 'cdr_sms') }}
    where sent_at >= {{ incremental_ts_lower_bound() }}::timestamp_tz
    qualify row_number() over (partition by sms_id order by ingested_at desc nulls last) = 1
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
        {{ tz_to_utc('ingested_at') }}                      as ingested_at_utc
    from src
    where subscription_id is not null
)
select *, sent_at_local::date as event_date
from shaped
where {{ incremental_date_filter('sent_at_local::date', 'event_date') }}
