{{ config(
    tags=['silver', 'usage', 'cdc'],
    schema='silver',
    materialized='incremental',
    unique_key='session_id',
    incremental_strategy='merge',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns',
    full_refresh=false,
    post_hook=["{{ cdc_log_changed_dates('event_date') }}",
               "{{ cdc_log_deletes('session_id', 'event_date') }}",
               "{{ cdc_purge_tombstones() }}"]
) }}
-- Data sessions. Bronze SESSION_START/END are TIMESTAMP_NTZ in UTC. CDC from bronze (see cdc.sql).
with src as (
    select *
    from {{ cdc_source('usage', 'cdr_data', "session_start >= " ~ incremental_ts_lower_bound()) }}
    qualify row_number() over (partition by session_id order by iff(_cdc_action = 'DELETE', 1, 0) desc, ingested_at desc nulls last) = 1
),
shaped as (
    select
        session_id,
        subscription_id,
        cell_id,
        {{ utc_ntz('session_start') }}                      as session_start_utc,
        {{ utc_to_local(utc_ntz('session_start')) }}        as session_start_local,
        greatest(datediff('second', session_start, session_end), 0) as duration_sec,
        zeroifnull(bytes_up)                                as bytes_up,
        zeroifnull(bytes_down)                              as bytes_down,
        zeroifnull(bytes_up) + zeroifnull(bytes_down)       as bytes_total,
        {{ bytes_to_mb('(zeroifnull(bytes_up) + zeroifnull(bytes_down))') }} as data_mb,
        nullif(upper(trim(apn)), '')                        as apn,
        upper(rat)                                          as rat,
        coalesce(is_roaming, false)                         as is_roaming,
        {{ tz_to_utc('ingested_at') }}                      as ingested_at_utc,
        _cdc_action
    from src
    where subscription_id is not null
)
select * exclude (_cdc_action), session_start_local::date as event_date, _cdc_action, {{ dbt_loaded_at() }} as _dbt_loaded_at
from shaped
where {{ full_refresh_floor('session_start_local::date') }}
