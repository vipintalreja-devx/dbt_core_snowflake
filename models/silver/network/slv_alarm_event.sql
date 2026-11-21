{{ config(
    materialized='incremental',
    schema='silver',
    tags=['silver', 'network', 'cdc'],
    transient=true,
    unique_key='alarm_id',
    incremental_strategy='merge',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('event_date') }}",
               "{{ cdc_log_deletes('alarm_id', 'event_date') }}",
               "{{ cdc_purge_tombstones() }}"]
) }}
-- Network alarms. CDC: new alarms and clears (CLEARED_AT updated later by the NMS).
select
    alarm_id,
    element_id,
    cell_id,
    alarm_code,
    upper(severity)                                         as severity,
    {{ tz_to_utc('raised_at') }}                            as raised_at_utc,
    {{ utc_to_local(tz_to_utc('raised_at')) }}::date        as event_date,
    {{ tz_to_utc('cleared_at') }}                           as cleared_at_utc,
    cleared_at is null                                      as is_active,
    datediff('minute', raised_at, cleared_at)               as duration_min,
    incident_id,
    description,
    _cdc_action,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ cdc_source('network', 'alarm_event', "raised_at >= dateadd('day', -1, '" ~ var('history_start_date') ~ "'::date)::timestamp_tz") }}
qualify row_number() over (partition by alarm_id order by iff(_cdc_action = 'DELETE', 1, 0) desc) = 1
