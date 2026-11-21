{%- set wm = cdc_watermark() -%}
{{ config(
    materialized='incremental',
    schema='silver',
    tags=['silver', 'care', 'cdc'],
    transient=true,
    unique_key='ticket_id',
    incremental_strategy='delete+insert',
    cluster_by=['ticket_id'],
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('changed_date', has_action=false) }}"]
) }}
-- Ticket status transitions with their sequence number per ticket.
-- CDC: change_seq is a per-ticket window, so every ticket with a new/changed status row
-- is recomputed over its full history (delete+insert on ticket_id).
select
    ticket_id,
    upper(status)                                           as status,
    {{ local_ntz_to_utc('changed_at') }}                    as changed_at_utc,
    changed_at::timestamp_ntz                               as changed_at_local,
    changed_at::date                                        as changed_date,
    changed_by,
    row_number() over (partition by ticket_id order by changed_at) as change_seq,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ source('care', 'ticket_status_history') }}
{% if wm -%}
where ticket_id in (select distinct ticket_id
                      from {{ source('care', 'ticket_status_history') }} changes(information => default) at(timestamp => '{{ wm }}'::timestamp_tz))
{%- endif %}
