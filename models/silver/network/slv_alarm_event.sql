{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'network'],
    transient=true,
    cluster_by=['event_date']
) }}
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
    description
from {{ source('network', 'alarm_event') }}
where raised_at >= dateadd('day', -1, '{{ var("history_start_date") }}'::date)::timestamp_tz
