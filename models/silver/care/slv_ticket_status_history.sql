{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'care'],
    transient=true,
    cluster_by=['ticket_id']
) }}
select
    ticket_id,
    upper(status)                                           as status,
    {{ local_ntz_to_utc('changed_at') }}                    as changed_at_utc,
    changed_at::timestamp_ntz                               as changed_at_local,
    changed_by,
    row_number() over (partition by ticket_id order by changed_at) as change_seq
from {{ source('care', 'ticket_status_history') }}
