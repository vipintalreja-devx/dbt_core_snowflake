{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['gold', 'aggregates', 'network', 'fault_management', 'e20']
) }}
-- Network alarms per Manila day x severity x alarm code x equipment vendor, with clearance times
-- and the regions most affected.
-- Ported from x_e20_alarm_severity_daily (PIPE_X.SP_X_E20_ALARM_SEVERITY_DAILY).
-- Grain   : event_date x severity x alarm_code x vendor
-- Sources : slv_alarm_event, slv_network_element, dim_cell, slv_outage_incident
with a as (
    select
        al.alarm_id, al.event_date, al.severity, al.alarm_code, al.cell_id, al.element_id,
        al.duration_min, al.is_active, al.incident_id,
        coalesce(ne.vendor, 'UNKNOWN')                      as vendor,
        coalesce(c.region_name, 'UNKNOWN')                  as region_name,
        oi.severity                                         as incident_severity
    from {{ ref('slv_alarm_event') }} al
    left join {{ ref('slv_network_element') }} ne on ne.element_id = al.element_id
    left join {{ ref('dim_cell') }} c             on c.cell_id = al.cell_id
    left join {{ ref('slv_outage_incident') }} oi on oi.incident_id = al.incident_id
    where al.event_date >= {{ f1_lower_bound('event_date') }}
),
region_counts as (
    select event_date, severity, alarm_code, vendor, region_name, count(*) as region_alarms
    from a group by 1, 2, 3, 4, 5
),
top_regions as (
    select event_date, severity, alarm_code, vendor,
           array_slice(array_agg(region_name) within group (order by region_alarms desc, region_name), 0, 3) as top_regions
    from region_counts group by 1, 2, 3, 4
)
select
    a.event_date, a.severity, a.alarm_code, a.vendor,
    count(*)                                                as alarms,
    approx_count_distinct(a.cell_id)                        as cells_approx,
    approx_count_distinct(a.element_id)                     as elements_approx,
    round(avg(a.duration_min), 0)                           as avg_minutes_to_clear,
    median(a.duration_min)                                  as median_minutes_to_clear,
    approx_percentile(a.duration_min, 0.9)                  as p90_minutes_to_clear,
    count_if(a.is_active)                                   as still_active,
    count_if(a.incident_id is not null)                     as linked_to_incident,
    count_if(a.incident_severity in ('S1', 'S2'))           as linked_to_major_incident,
    any_value(t.top_regions)                                as top_regions,
    round(ratio_to_report(count(*)) over (partition by a.event_date), 6) as share_of_day_alarms,
    current_timestamp()                                     as loaded_at
from a
left join top_regions t
       on t.event_date = a.event_date and t.severity = a.severity
      and t.alarm_code = a.alarm_code and t.vendor = a.vendor
group by 1, 2, 3, 4
