{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ticket_id',
    on_schema_change='append_new_columns',
    schema='gold',
    tags=['gold', 'facts', 'fact'],
    transient=false,
    cluster_by=['opened_date'],
    copy_grants=true
) }}
-- Grain: care ticket, with SLA attainment, re-opens, field work and NPS feedback.
-- Incremental: tickets whose header, status history, work orders or NPS changed since the
-- last run are recomputed over ALL their child rows; still-open tickets are always
-- recomputed because is_open_breached depends on the current time.
with changed as (
    select ticket_id from {{ cdc_changed_keys(ref('slv_ticket'), 'ticket_id') }}
    union select ticket_id from {{ cdc_changed_keys(ref('slv_ticket_status_history'), 'ticket_id') }}
    union select ticket_id from {{ cdc_changed_keys(ref('slv_work_order'), 'ticket_id') }}
    union select ticket_id from {{ cdc_changed_keys(ref('slv_nps_survey'), 'ticket_id') }}
    {%- if is_incremental() %}
    union select ticket_id from {{ ref('slv_ticket') }} where is_open
    {%- endif %}
),
hist as (
    select ticket_id,
           count(*)                                         as status_changes,
           count_if(status = 'OPEN' and change_seq > 1)     as reopen_count
    from {{ ref('slv_ticket_status_history') }}
    where ticket_id in (select ticket_id from changed)
    group by 1
),
wo as (
    select ticket_id, count(*) as work_orders, max(iff(is_first_time_fix, 1, 0)) = 1 as had_fix
    from {{ ref('slv_work_order') }}
    where ticket_id in (select ticket_id from changed)
    group by 1
),
nps as (
    select ticket_id, nps_score, nps_category
    from {{ ref('slv_nps_survey') }}
    where ticket_id is not null
      and ticket_id in (select ticket_id from changed)
    qualify row_number() over (partition by ticket_id order by responded_at_utc desc) = 1
)
select
    t.ticket_id,
    t.subscription_id,
    t.opened_date,
    t.opened_at_local,
    t.resolved_date,
    t.channel,
    t.category,
    t.priority,
    t.status,
    t.is_open,
    t.resolution_hours,
    sla.resolution_sla_hours,
    case when t.resolution_hours is null then null
         else t.resolution_hours <= sla.resolution_sla_hours end as is_within_sla,
    t.is_open and datediff('hour', t.opened_at_utc, sysdate()) > sla.resolution_sla_hours as is_open_breached,
    zeroifnull(h.status_changes)                            as status_changes,
    zeroifnull(h.reopen_count)                              as reopen_count,
    zeroifnull(w.work_orders)                               as work_orders,
    coalesce(w.had_fix, false)                              as had_field_fix,
    n.nps_score,
    n.nps_category
from {{ ref('slv_ticket') }} t
left join {{ ref('sla_policy') }} sla on sla.priority = t.priority
left join hist h on h.ticket_id = t.ticket_id
left join wo   w on w.ticket_id = t.ticket_id
left join nps  n on n.ticket_id = t.ticket_id
where t.ticket_id in (select ticket_id from changed)
