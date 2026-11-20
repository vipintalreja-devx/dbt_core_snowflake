{{ config(
    tags=['silver', 'usage'],
    schema='silver',
    materialized='incremental',
    unique_key='call_id',
    incremental_strategy='merge',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns'
) }}
-- Voice CDRs: deduplicated on CALL_ID (latest ingest wins), timezone-normalised
-- (bronze CALL_START is TIMESTAMP_TZ with mixed offsets), MSISDNs normalised.
with src as (
    select *
    from {{ source('usage', 'cdr_voice') }}
    where call_start >= {{ incremental_ts_lower_bound() }}::timestamp_tz
    qualify row_number() over (partition by call_id order by ingested_at desc nulls last) = 1
),
shaped as (
    select
        call_id,
        subscription_id,
        {{ normalize_msisdn('calling_msisdn') }}            as calling_msisdn,
        {{ normalize_msisdn('called_msisdn') }}             as called_msisdn,
        cell_id,
        {{ tz_to_utc('call_start') }}                       as call_start_utc,
        {{ utc_to_local(tz_to_utc('call_start')) }}         as call_start_local,
        duration_sec,
        upper(call_type)                                    as call_type,
        coalesce(is_roaming, false)                         as is_roaming,
        iff(coalesce(is_roaming, false), roaming_country, 'PH') as call_country,
        charge_php::number(18, 4)                           as charge_php,
        {{ tz_to_utc('ingested_at') }}                      as ingested_at_utc
    from src
    where duration_sec >= 0
      and subscription_id is not null
)
select
    s.*,
    s.call_start_local::date                                as event_date,
    floor((hour(s.call_start_local) * 60 + minute(s.call_start_local)) / 30) + 1 as slot_id,
    coalesce(ts.is_peak, false)                             as is_peak
from shaped s
left join {{ ref('slv_time_slot') }} ts
       on ts.slot_id = floor((hour(s.call_start_local) * 60 + minute(s.call_start_local)) / 30) + 1
where {{ incremental_date_filter('s.call_start_local::date', 'event_date') }}
