{{ config(
    materialized='incremental',
    schema='gold',
    tags=['insights', 'p09'],
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    transient=false
) }}
-- P09 coverage gaps | step 3: FTTH optical health per barangay and day.
-- Grain   : city x barangay x event_date (region_id = dominant region of the barangay)
-- Sources : slv_ont_health_daily (CDC silver), int_p09_service_location (ONT -> barangay / OLT)
with ont as (
    select ont_serial, event_date, polls, rx_dbm_min, rx_dbm_avg, temperature_c_max, low_rx_polls, is_degraded
    from {{ ref('slv_ont_health_daily') }}
    where event_date >= {{ f1_lower_bound('event_date') }}
),
loc as (
    select ont_serial, barangay, city, region_id, region_name, serving_olt_id, serving_olt_distance_m
    from {{ ref('int_p09_service_location') }}
    where service_type = 'FTTH' and ont_serial is not null
)
select
    l.region_id,
    l.region_name,
    l.city,
    l.barangay,
    o.event_date,
    count(*)                                                as onts_reporting,
    count_if(o.is_degraded)                                 as onts_degraded,
    round(count_if(o.is_degraded) / count(*), 4)            as degraded_share,
    round(avg(o.rx_dbm_avg), 2)                             as rx_dbm_avg,
    round(approx_percentile(o.rx_dbm_min, 0.05), 2)         as rx_dbm_p05,
    sum(o.low_rx_polls)                                     as low_rx_polls,
    count_if(o.temperature_c_max >= 60)                     as onts_overheating,
    count(distinct l.serving_olt_id)                        as olts_serving,
    round(avg(l.serving_olt_distance_m))                    as avg_olt_distance_m,
    round(corr(l.serving_olt_distance_m, o.rx_dbm_avg), 3)  as corr_distance_vs_rx
from ont o
join loc l on l.ont_serial = o.ont_serial
group by 1, 2, 3, 4, 5
