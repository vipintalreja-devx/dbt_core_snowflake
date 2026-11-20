{{ config(
    materialized='table',
    tags=['gold', 'analytics', 'care', 'field_force', 'm08']
) }}
-- Appointment adherence per month, region and booked half-hour slot: confirmed share, on-time
-- arrivals, early / late arrivals and lateness distribution.
-- Ported from x_m08_appointment_adherence_monthly (PIPE_X.SP_X_M08_APPOINTMENT_ADHERENCE_MONTHLY).
-- Grain: slot_month x region_name x slot_label.
-- Sources: slv_work_order, slv_time_slot, slv_field_technician, slv_region.
with appt as (
    select
        date_trunc('month', w.slot_date)::date              as slot_month,
        coalesce(r.region_name, 'UNKNOWN')                  as region_name,
        coalesce(ts.slot_label, 'UNKNOWN')                  as slot_label,
        coalesce(ts.is_peak, false)                         as is_peak_slot,
        w.is_slot_confirmed,
        w.arrived_at_local,
        w.slot_start_local,
        w.slot_end_local,
        case
            when w.arrived_at_local is null                         then 'NO_ARRIVAL'
            when w.arrived_at_local <  w.slot_start_local           then 'EARLY'
            when w.arrived_at_local <= w.slot_end_local             then 'ON_TIME'
            else 'LATE'
        end                                                 as arrival_status,
        iff(w.arrived_at_local > w.slot_end_local,
            datediff('minute', w.slot_end_local, w.arrived_at_local), null) as late_minutes
    from {{ ref('slv_work_order') }} w
    left join {{ ref('slv_time_slot') }} ts          on ts.slot_id = w.slot_id
    left join {{ ref('slv_field_technician') }} t    on t.technician_id = w.technician_id
    left join {{ ref('slv_region') }} r              on r.region_id = t.region_id
    where w.slot_date is not null
)
select
    slot_month,
    region_name,
    slot_label,
    is_peak_slot,
    count(*)                                                as appointments,
    count_if(is_slot_confirmed)                             as confirmed,
    count_if(arrival_status = 'ON_TIME')                    as on_time,
    count_if(arrival_status = 'EARLY')                      as early,
    count_if(arrival_status = 'LATE')                       as late,
    count_if(arrival_status = 'NO_ARRIVAL')                 as no_arrival,
    round(div0(count_if(arrival_status = 'ON_TIME'), count(arrived_at_local)), 4) as on_time_rate,
    round(div0(count_if(is_slot_confirmed), count(*)), 4)   as confirmed_rate,
    round(avg(late_minutes), 1)                             as avg_late_minutes,
    approx_percentile(late_minutes, 0.9)                    as p90_late_minutes,
    current_timestamp()                                     as loaded_at
from appt
group by 1, 2, 3, 4
