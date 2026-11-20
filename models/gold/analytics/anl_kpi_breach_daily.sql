{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['gold', 'analytics', 'network', 'kpi', 'm16']
) }}
-- Radio-KPI threshold breaches per cell, KPI and day. Hourly KPIs are UNPIVOTed to long form and
-- compared with the per-cell thresholds (direction-aware: '<' for throughput, '>' for the rest).
-- Ported from x_m16_kpi_breach_daily (PIPE_X.SP_X_M16_KPI_BREACH_DAILY); hourly instead of 15-min
-- grain because silver already rolled PM counters up and fixed units (drop rate in %).
-- Grain: event_date x cell_id x kpi_name (only cell/KPI/days with >= 1 warning hour).
-- Sources: slv_cell_kpi_hourly, source network.cell_kpi_threshold, dim_cell, slv_alarm_event.
with hourly_long as (
    select cell_id, event_date, hour_local, kpi_name, kpi_value
    from (
        select cell_id, event_date, hour_local,
               throughput_mbps_avg  as avg_throughput,
               drop_rate_pct_avg    as drop_call_rate,
               latency_ms_avg       as latency,
               prb_pct_avg          as prb_utilization
        from {{ ref('slv_cell_kpi_hourly') }}
        where {{ incremental_date_filter('event_date') }}
    )
    unpivot (kpi_value for kpi_name in (avg_throughput, drop_call_rate, latency, prb_utilization))
),
thr as (
    select cell_id, upper(kpi_name) as kpi_name, comparison, warning_threshold, critical_threshold, unit
    from {{ source('network', 'cell_kpi_threshold') }}
    where coalesce(is_enabled, true)
),
flagged as (
    select
        h.cell_id, h.event_date, h.hour_local, h.kpi_name, h.kpi_value, t.unit,
        t.warning_threshold, t.critical_threshold,
        iff(t.comparison = '<', h.kpi_value < t.warning_threshold,  h.kpi_value > t.warning_threshold)  as is_warning,
        iff(t.comparison = '<', h.kpi_value < t.critical_threshold, h.kpi_value > t.critical_threshold) as is_critical
    from hourly_long h
    join thr t on t.cell_id = h.cell_id and t.kpi_name = upper(h.kpi_name)
),
alarms as (
    select cell_id, event_date, count(*) as alarms, count_if(severity in ('CRITICAL', 'MAJOR')) as major_alarms
    from {{ ref('slv_alarm_event') }}
    where {{ incremental_date_filter('event_date') }}
    group by 1, 2
)
select
    f.event_date,
    f.cell_id,
    c.site_name,
    c.region_name,
    c.radio_type,
    upper(f.kpi_name)                                       as kpi_name,
    any_value(f.unit)                                       as unit,
    any_value(f.warning_threshold)                          as warning_threshold,
    any_value(f.critical_threshold)                         as critical_threshold,
    count(*)                                                as hours_measured,
    count_if(f.is_warning)                                  as warning_hours,
    count_if(f.is_critical)                                 as critical_hours,
    round(min(f.kpi_value), 4)                              as min_value,
    round(max(f.kpi_value), 4)                              as max_value,
    round(avg(f.kpi_value), 4)                              as avg_value,
    min_by(hour(f.hour_local), iff(f.is_warning, f.hour_local, null)) as first_breach_hour,
    -- longest consecutive run of warning hours (gaps-and-islands)
    max(run_len)                                            as longest_breach_run_hours,
    zeroifnull(any_value(a.alarms))                         as alarms_same_day,
    zeroifnull(any_value(a.major_alarms))                   as major_alarms_same_day,
    current_timestamp()                                     as loaded_at
from (
    select f.*,
           iff(f.is_warning,
               count(*) over (partition by f.cell_id, f.kpi_name, f.event_date, f.grp), 0) as run_len
    from (
        select f.*,
               row_number() over (partition by f.cell_id, f.kpi_name, f.event_date order by f.hour_local)
             - row_number() over (partition by f.cell_id, f.kpi_name, f.event_date, f.is_warning order by f.hour_local) as grp
        from flagged f
    ) f
) f
left join {{ ref('dim_cell') }} c on c.cell_id = f.cell_id
left join alarms a on a.cell_id = f.cell_id and a.event_date = f.event_date
group by f.event_date, f.cell_id, c.site_name, c.region_name, c.radio_type, upper(f.kpi_name)
having count_if(f.is_warning) > 0
