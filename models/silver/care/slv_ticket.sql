{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'care'],
    transient=true
) }}
select
    ticket_id,
    subscription_id,
    {{ tz_to_utc('opened_at') }}                            as opened_at_utc,
    {{ utc_to_local(tz_to_utc('opened_at')) }}              as opened_at_local,
    {{ utc_to_local(tz_to_utc('opened_at')) }}::date        as opened_date,
    upper(channel)                                          as channel,
    upper(category)                                         as category,
    upper(priority)                                         as priority,
    upper(status)                                           as status,
    subject,
    {{ tz_to_utc('resolved_at') }}                          as resolved_at_utc,
    {{ utc_to_local(tz_to_utc('resolved_at')) }}::date      as resolved_date,
    upper(status) = 'OPEN'                                  as is_open,
    round(datediff('second', opened_at, resolved_at) / 3600, 2) as resolution_hours
from {{ source('care', 'ticket') }}
