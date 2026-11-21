{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='work_order_id',
    on_schema_change='append_new_columns',
    schema='gold',
    tags=['gold', 'facts', 'fact'],
    transient=false,
    copy_grants=true
) }}
-- Incremental: work orders changed in silver since the last run (merge on work_order_id).
select
    w.work_order_id, w.ticket_id, w.technician_id, t.region_id, w.work_type, w.outcome,
    w.created_date, w.slot_date, w.hours_to_arrival, w.hours_on_site,
    w.is_slot_confirmed, w.is_arrived_in_slot, w.is_first_time_fix
from {{ ref('slv_work_order') }} w
left join {{ ref('slv_field_technician') }} t on t.technician_id = w.technician_id
where {{ cdc_rows_since_watermark('w') }}
