{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'ref'],
    transient=true
) }}
-- 48 half-hour slots; slot_id = floor(minute_of_day / 30) + 1 (equi-join key).
select slot_id, slot_label, slot_start, slot_end, coalesce(is_peak, false) as is_peak
from {{ source('ref', 'time_slot') }}
