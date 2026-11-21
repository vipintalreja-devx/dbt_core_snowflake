{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='change_date',
    on_schema_change='append_new_columns',
    cluster_by=['change_date'],
    tags=['gold', 'aggregates', 'care', 'workflow', 'e28']
) }}
-- Ticket workflow transitions per day x from-status x to-status x actor x priority, with dwell time
-- in the previous status and re-open detection (CONDITIONAL_CHANGE_EVENT over each ticket's history).
-- Ported from x_e28_ticket_status_changes_daily (PIPE_X.SP_X_E28_TICKET_STATUS_CHANGES_DAILY).
-- Grain   : change_date x from_status x status x changed_by x priority
-- Sources : slv_ticket_status_history, slv_ticket, sla_policy (seed)
with h as (
    select
        sh.ticket_id, sh.status, sh.changed_by, sh.changed_at_local, sh.change_seq,
        lag(sh.status) over (partition by sh.ticket_id order by sh.changed_at_local, sh.change_seq) as from_status,
        lag(sh.changed_at_local) over (partition by sh.ticket_id order by sh.changed_at_local, sh.change_seq) as prev_changed_at_local,
        conditional_change_event(sh.status) over (partition by sh.ticket_id order by sh.changed_at_local, sh.change_seq) as status_change_no
    from {{ ref('slv_ticket_status_history') }} sh
),
t as (
    select
        h.*,
        h.changed_at_local::date                            as change_date,
        datediff('minute', h.prev_changed_at_local, h.changed_at_local) / 60 as hours_in_prev_status,
        h.status = 'OPEN' and h.from_status in ('RESOLVED', 'CLOSED') as is_reopen,
        tk.priority, tk.category, tk.channel,
        sla.resolution_sla_hours,
        datediff('minute', tk.opened_at_local, h.changed_at_local) / 60 as hours_since_open
    from h
    left join {{ ref('slv_ticket') }} tk   on tk.ticket_id = h.ticket_id
    left join {{ ref('sla_policy') }} sla on sla.priority = tk.priority
    -- lag()/conditional_change_event() above run over each ticket's FULL history; only the
    -- output days are restricted to the incremental window
    where h.changed_at_local::date >= {{ f1_lower_bound('change_date') }}
)
select
    change_date,
    coalesce(from_status, 'NEW')                            as from_status,
    status,
    coalesce(changed_by, 'UNKNOWN')                         as changed_by,
    coalesce(priority, 'UNKNOWN')                           as priority,
    count(*)                                                as changes,
    approx_count_distinct(ticket_id)                        as tickets_approx,
    count_if(is_reopen)                                     as reopens,
    round(avg(hours_in_prev_status), 2)                     as avg_hours_in_prev_status,
    median(hours_in_prev_status)                            as median_hours_in_prev_status,
    count_if(status in ('RESOLVED', 'CLOSED') and hours_since_open > resolution_sla_hours) as resolved_after_sla,
    max(status_change_no)                                   as max_status_changes_on_a_ticket,
    mode(category)                                          as top_category,
    round(ratio_to_report(count(*)) over (partition by change_date), 6) as share_of_day_changes,
    current_timestamp()                                     as loaded_at
from t
group by 1, 2, 3, 4, 5
