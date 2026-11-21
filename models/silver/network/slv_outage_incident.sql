{{ config(
    materialized='incremental',
    schema='silver',
    tags=['silver', 'network', 'cdc'],
    transient=true,
    unique_key='incident_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('event_date') }}",
               "{{ cdc_log_deletes('incident_id', 'event_date') }}",
               "{{ cdc_purge_tombstones() }}"]
) }}
-- Network outages. CDC: new incidents and restorations (RESTORED_AT set later).
select
    incident_id,
    upper(severity)                                         as severity,
    region_id,
    {{ tz_to_utc('detected_at') }}                          as detected_at_utc,
    {{ utc_to_local(tz_to_utc('detected_at')) }}::date      as event_date,
    {{ tz_to_utc('restored_at') }}                          as restored_at_utc,
    datediff('minute', detected_at, restored_at)            as outage_minutes,
    restored_at is null                                     as is_ongoing,
    root_cause,
    affected_area,
    _cdc_action,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ cdc_source('network', 'outage_incident') }}
qualify row_number() over (partition by incident_id order by iff(_cdc_action = 'DELETE', 1, 0) desc) = 1
