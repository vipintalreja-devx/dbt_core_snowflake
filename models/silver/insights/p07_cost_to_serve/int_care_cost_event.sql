{{ config(
    materialized='incremental',
    schema='silver',
    tags=['insights', 'p07', 'care', 'cost'],
    incremental_strategy='delete+insert',
    unique_key='event_date',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns'
) }}
-- P07 Cost-to-serve: every care activity priced with the unit costs in seed cost_to_serve_rates
-- (documented assumptions).
--   TICKET      handling by channel x priority multiplier
--   REOPEN      each re-open of a ticket (status history)
--   NOTE        each interaction note, by author type (agent / team lead / technician / bot)
--   FIELD       work order base by work type + on-site hours x (employee | contractor) rate
--               + travel when dispatched + CPE replacement + reschedule
-- Grain   : cost_event_id (one row per priced activity), event_date = Manila business day
-- Sources : slv_ticket, slv_ticket_status_history, care_notes.interaction_note, slv_work_order,
--           slv_field_technician, cost_to_serve_rates (seed)
with rates as (
    select cost_type, cost_key, cost_php from {{ ref('cost_to_serve_rates') }}
),
tickets as (
    select t.ticket_id, t.subscription_id, t.opened_date as event_date, t.channel, t.priority
    from {{ ref('slv_ticket') }} t
    where t.opened_date >= {{ f1_lower_bound('event_date') }}
),
ticket_cost as (
    select 'TICKET-' || t.ticket_id                                         as cost_event_id,
           t.event_date, t.subscription_id, t.ticket_id, null::number as work_order_id,
           'TICKET' as cost_type, t.channel || '/' || t.priority as cost_driver, 1 as quantity,
           round(coalesce(c.cost_php, 60) * coalesce(p.cost_php, 1), 2)     as cost_php
    from tickets t
    left join rates c on c.cost_type = 'TICKET' and c.cost_key = t.channel
    left join rates p on p.cost_type = 'PRIORITY' and p.cost_key = t.priority
),
ticket_lookup as (
    select ticket_id, subscription_id from {{ ref('slv_ticket') }}
),
reopen_cost as (
    select 'REOPEN-' || h.ticket_id || '-' || h.change_seq                  as cost_event_id,
           h.changed_date as event_date, tl.subscription_id, h.ticket_id, null::number,
           'REOPEN', 'REOPEN', 1, (select cost_php from rates where cost_type = 'REOPEN' and cost_key = 'ANY')
    from {{ ref('slv_ticket_status_history') }} h
    join ticket_lookup tl on tl.ticket_id = h.ticket_id
    where h.status = 'OPEN' and h.change_seq > 1
      and h.changed_date >= {{ f1_lower_bound('event_date') }}
),
note_cost as (
    select 'NOTE-' || n.note_id                                             as cost_event_id,
           {{ utc_to_local(tz_to_utc('n.created_at')) }}::date              as event_date,
           tl.subscription_id, n.ticket_id, null::number,
           'NOTE', upper(split_part(n.author, '_', 1)), 1,
           coalesce(r.cost_php, 18)                                         as cost_php
    from {{ source('care_notes', 'interaction_note') }} n
    join ticket_lookup tl on tl.ticket_id = n.ticket_id
    left join rates r on r.cost_type = 'NOTE' and r.cost_key = upper(split_part(n.author, '_', 1))
    where n.created_at >= dateadd('day', -1, {{ f1_lower_bound('event_date') }})::timestamp_tz
      and {{ utc_to_local(tz_to_utc('n.created_at')) }}::date >= {{ f1_lower_bound('event_date') }}
),
work_orders as (
    select w.work_order_id, w.ticket_id, tl.subscription_id, w.created_date as event_date, w.work_type, w.outcome,
           w.dispatched_at_local is not null                                as was_dispatched,
           -- ~37% of source work orders have completed_at before arrived_at: negative on-site
           -- time is a source data defect and is costed as 0 hours (base + travel still apply)
           greatest(zeroifnull(w.hours_on_site), 0)                         as hours_on_site,
           coalesce(ft.is_contractor, false)                                as is_contractor
    from {{ ref('slv_work_order') }} w
    left join ticket_lookup tl on tl.ticket_id = w.ticket_id
    left join {{ ref('slv_field_technician') }} ft on ft.technician_id = w.technician_id
    where w.created_date >= {{ f1_lower_bound('event_date') }}
),
field_cost as (
    select 'FIELD-' || wo.work_order_id                                     as cost_event_id,
           wo.event_date, wo.subscription_id, wo.ticket_id, wo.work_order_id,
           'FIELD', wo.work_type || coalesce('/' || wo.outcome, ''), 1,
           round(coalesce(base.cost_php, oth.cost_php)
                 + wo.hours_on_site * iff(wo.is_contractor, hc.cost_php, he.cost_php)
                 + iff(wo.was_dispatched, tr.cost_php, 0)
                 + iff(wo.outcome = 'REPLACED_CPE', cpe.cost_php, 0)
                 + iff(wo.outcome = 'RESCHEDULED', rs.cost_php, 0), 2)      as cost_php
    from work_orders wo
    left join rates base on base.cost_type = 'WORK_ORDER' and base.cost_key = wo.work_type
    join rates oth on oth.cost_type = 'WORK_ORDER' and oth.cost_key = 'OTHER'
    join rates he  on he.cost_type = 'TECH_HOUR' and he.cost_key = 'EMPLOYEE'
    join rates hc  on hc.cost_type = 'TECH_HOUR' and hc.cost_key = 'CONTRACTOR'
    join rates tr  on tr.cost_type = 'TRAVEL' and tr.cost_key = 'ANY'
    join rates cpe on cpe.cost_type = 'CPE' and cpe.cost_key = 'REPLACED_CPE'
    join rates rs  on rs.cost_type = 'RESCHEDULE' and rs.cost_key = 'RESCHEDULED'
)
select cost_event_id, event_date, subscription_id, ticket_id, work_order_id, cost_type, cost_driver, quantity, cost_php,
       {{ dbt_loaded_at() }} as _dbt_loaded_at
from (
    select * from ticket_cost
    union all select * from reopen_cost
    union all select * from note_cost
    union all select * from field_cost
)
where subscription_id is not null
