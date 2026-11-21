{{ config(
    materialized='incremental',
    schema='silver',
    tags=['silver', 'care', 'cdc'],
    transient=true,
    unique_key='ticket_id',
    incremental_strategy='merge',
    cluster_by=['opened_date'],
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('opened_date') }}",
               "{{ cdc_log_deletes('ticket_id', 'opened_date') }}",
               "{{ cdc_purge_tombstones() }}"]
) }}
-- Care tickets. CDC: new tickets and lifecycle updates (status / resolved_at).
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
    round(datediff('second', opened_at, resolved_at) / 3600, 2) as resolution_hours,
    _cdc_action,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ cdc_source('care', 'ticket') }}
qualify row_number() over (partition by ticket_id order by iff(_cdc_action = 'DELETE', 1, 0) desc) = 1
