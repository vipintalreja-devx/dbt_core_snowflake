{{ config(
    materialized='incremental',
    schema='silver',
    tags=['silver', 'care', 'cdc'],
    transient=true,
    unique_key='work_order_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('created_date') }}",
               "{{ cdc_log_deletes('work_order_id', 'created_date') }}",
               "{{ cdc_purge_tombstones() }}"]
) }}
-- Field work orders with their booked appointment slot (bronze times are Manila local NTZ). CDC from bronze.
select
    wo.work_order_id,
    wo.ticket_id,
    wo.technician_id,
    upper(wo.work_type)                                     as work_type,
    upper(wo.outcome)                                       as outcome,
    wo.created_at::timestamp_ntz                            as created_at_local,
    wo.dispatched_at::timestamp_ntz                         as dispatched_at_local,
    wo.arrived_at::timestamp_ntz                            as arrived_at_local,
    wo.completed_at::timestamp_ntz                          as completed_at_local,
    wo.created_at::date                                     as created_date,
    ap.slot_id,
    ap.slot_date,
    timestamp_ntz_from_parts(ap.slot_date, ap.slot_start)   as slot_start_local,
    timestamp_ntz_from_parts(ap.slot_date, ap.slot_end)     as slot_end_local,
    coalesce(ap.confirmed, false)                           as is_slot_confirmed,
    round(datediff('minute', wo.created_at, wo.arrived_at) / 60, 2)    as hours_to_arrival,
    round(datediff('minute', wo.arrived_at, wo.completed_at) / 60, 2)  as hours_on_site,
    case
        when wo.arrived_at is null or ap.slot_date is null then null
        when wo.arrived_at <= timestamp_ntz_from_parts(ap.slot_date, ap.slot_end) then true
        else false
    end                                                     as is_arrived_in_slot,
    upper(wo.outcome) in ('FIXED', 'REPLACED_CPE')          as is_first_time_fix,
    wo._cdc_action,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ cdc_source('care', 'work_order') }} wo
left join {{ source('care', 'work_order_appointment') }} ap
       on ap.work_order_id = wo.work_order_id
qualify row_number() over (partition by wo.work_order_id order by iff(wo._cdc_action = 'DELETE', 1, 0) desc, ap.slot_id) = 1
