{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'network'],
    transient=true
) }}
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
    affected_area
from {{ source('network', 'outage_incident') }}
