{{ config(
    materialized='table',
    cluster_by=['work_month'],
    tags=['gold', 'analytics', 'care', 'field_force', 'm07']
) }}
-- Field-technician productivity per month: jobs, first-time-fix, travel / on-site time, priority mix,
-- slot adherence and a within-region ranking (all months; work orders are a small table).
-- Ported from x_m07_technician_monthly (PIPE_X.SP_X_M07_TECHNICIAN_MONTHLY).
-- Grain: technician_id x work_month.
-- Sources: slv_work_order, slv_field_technician, slv_region, fct_ticket.
with wo as (
    select
        w.*,
        date_trunc('month', w.created_date)::date           as work_month,
        datediff('minute', w.dispatched_at_local, w.arrived_at_local)  as travel_min,
        datediff('minute', w.arrived_at_local, w.completed_at_local)   as onsite_min,
        t.priority,
        t.category
    from {{ ref('slv_work_order') }} w
    left join {{ ref('fct_ticket') }} t on t.ticket_id = w.ticket_id
),
agg as (
    select
        technician_id,
        work_month,
        count(*)                                            as jobs,
        count_if(work_type = 'REPAIR')                      as repair_jobs,
        count_if(work_type = 'INSPECTION')                  as inspection_jobs,
        count_if(is_first_time_fix)                         as fixed_jobs,
        count_if(outcome = 'RESCHEDULED')                   as rescheduled_jobs,
        count_if(outcome = 'NO_FAULT_FOUND')                as no_fault_found_jobs,
        count_if(priority = 'P1')                           as p1_jobs,
        count_if(is_arrived_in_slot)                        as arrived_in_slot,
        round(avg(travel_min), 1)                           as avg_travel_min,
        round(median(travel_min), 1)                        as median_travel_min,
        round(avg(onsite_min), 1)                           as avg_onsite_min,
        mode(category)                                      as top_ticket_category,
        count(distinct created_date)                        as working_days
    from wo
    group by 1, 2
)
select
    a.technician_id,
    a.work_month,
    t.technician_name,
    t.region_id,
    coalesce(r.region_name, 'UNKNOWN')                      as region_name,
    t.is_contractor,
    t.skill_count,
    datediff('month', t.hired_on, a.work_month)             as tenure_months,
    a.jobs,
    a.repair_jobs,
    a.inspection_jobs,
    a.fixed_jobs,
    a.rescheduled_jobs,
    a.no_fault_found_jobs,
    a.p1_jobs,
    a.working_days,
    round(div0(a.jobs, a.working_days), 2)                  as jobs_per_working_day,
    round(div0(a.fixed_jobs, a.jobs), 4)                    as first_time_fix_rate,
    round(div0(a.arrived_in_slot, a.jobs), 4)               as slot_adherence_rate,
    a.avg_travel_min,
    a.median_travel_min,
    a.avg_onsite_min,
    a.top_ticket_category,
    round(ratio_to_report(a.jobs) over (partition by a.work_month, t.region_id), 4) as share_of_region_jobs,
    dense_rank() over (partition by a.work_month, t.region_id order by div0(a.fixed_jobs, a.jobs) desc, a.jobs desc) as ftf_rank_in_region,
    a.jobs - lag(a.jobs) over (partition by a.technician_id order by a.work_month) as jobs_mom_change,
    current_timestamp()                                     as loaded_at
from agg a
join {{ ref('slv_field_technician') }} t on t.technician_id = a.technician_id
left join {{ ref('slv_region') }} r      on r.region_id = t.region_id
