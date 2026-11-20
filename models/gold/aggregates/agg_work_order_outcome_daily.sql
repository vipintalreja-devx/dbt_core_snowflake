{{ config(
    materialized='table',
    cluster_by=['completed_date'],
    tags=['gold', 'aggregates', 'care', 'field_service', 'e09']
) }}
-- Field work orders completed per day x work type x outcome x technician region.
-- Ported from x_e09_work_order_outcome_daily (PIPE_X.SP_X_E09_WORK_ORDER_OUTCOME_DAILY).
-- Grain   : completed_date x work_type x outcome x region_name
-- Sources : slv_work_order, slv_field_technician, dim_region, slv_ticket
with wo as (
    select
        w.work_order_id, w.ticket_id, w.technician_id, w.work_type, w.outcome,
        w.completed_at_local::date                          as completed_date,
        datediff('minute', w.created_at_local, w.completed_at_local) as minutes_to_complete,
        w.hours_to_arrival, w.hours_on_site, w.is_arrived_in_slot, w.is_slot_confirmed, w.is_first_time_fix,
        t.is_contractor,
        t.skill_count,
        coalesce(r.region_name, 'UNKNOWN')                  as region_name,
        tk.category                                         as ticket_category,
        tk.priority                                         as ticket_priority
    from {{ ref('slv_work_order') }} w
    left join {{ ref('slv_field_technician') }} t on t.technician_id = w.technician_id
    left join {{ ref('dim_region') }} r          on r.region_id = t.region_id
    left join {{ ref('slv_ticket') }} tk         on tk.ticket_id = w.ticket_id
    where w.completed_at_local >= '{{ var("history_start_date") }}'::timestamp_ntz
)
select
    completed_date, work_type, outcome, region_name,
    count(*)                                                as orders,
    approx_count_distinct(technician_id)                    as technicians_approx,
    round(avg(minutes_to_complete), 0)                      as avg_minutes_to_complete,
    approx_percentile(minutes_to_complete, 0.9)             as p90_minutes_to_complete,
    median(hours_to_arrival)                                as median_hours_to_arrival,
    round(avg(hours_on_site), 2)                            as avg_hours_on_site,
    round(div0(count_if(is_arrived_in_slot), count(is_arrived_in_slot)) * 100, 2) as in_slot_arrival_pct,
    round(div0(count_if(is_slot_confirmed), count(*)) * 100, 2)  as slot_confirmed_pct,
    round(div0(count_if(is_first_time_fix), count(*)) * 100, 2)  as first_time_fix_pct,
    round(div0(count_if(is_contractor), count(*)) * 100, 2)      as contractor_share_pct,
    count_if(ticket_priority in ('P1', 'P2'))               as orders_high_priority,
    mode(ticket_category)                                   as top_ticket_category,
    round(ratio_to_report(count(*)) over (partition by completed_date), 6) as share_of_day_orders,
    current_timestamp()                                     as loaded_at
from wo
where completed_date is not null
group by 1, 2, 3, 4
