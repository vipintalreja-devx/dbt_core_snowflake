{{ config(
    tags=['gold', 'facts', 'fact'],
    schema='gold',
    materialized='incremental',
    unique_key='event_date',
    incremental_strategy='delete+insert',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns'
) }}
-- Grain: cell x local day. Radio KPIs vs. per-cell thresholds, alarms and outage exposure.
with kpi as (
    select cell_id, event_date,
           sum(pm_intervals)                                as pm_intervals,
           avg(prb_pct_avg)                                 as prb_pct_avg,
           max(prb_pct_max)                                 as prb_pct_peak,
           avg(throughput_mbps_avg)                         as throughput_mbps_avg,
           avg(latency_ms_avg)                              as latency_ms_avg,
           max(latency_ms_max)                              as latency_ms_peak,
           avg(drop_rate_pct_avg)                           as drop_rate_pct_avg,
           max(active_users_max)                            as active_users_peak,
           max_by(hour(hour_local), prb_pct_avg)            as busy_hour
    from {{ ref('slv_cell_kpi_hourly') }}
    where {{ incremental_date_filter('event_date') }}
    group by 1, 2
),
alarms as (
    select cell_id, event_date,
           count(*)                                         as alarms,
           count_if(severity in ('CRITICAL', 'MAJOR'))      as major_alarms,
           sum(duration_min)                                as alarm_minutes
    from {{ ref('slv_alarm_event') }}
    where {{ incremental_date_filter('event_date') }}
    group by 1, 2
)
select
    k.cell_id,
    k.event_date,
    k.pm_intervals,
    round(k.pm_intervals / 96, 4)                           as pm_completeness,
    k.prb_pct_avg,
    k.prb_pct_peak,
    k.throughput_mbps_avg,
    k.latency_ms_avg,
    k.latency_ms_peak,
    k.drop_rate_pct_avg,
    k.active_users_peak,
    k.busy_hour,
    zeroifnull(a.alarms)                                    as alarms,
    zeroifnull(a.major_alarms)                              as major_alarms,
    zeroifnull(a.alarm_minutes)                             as alarm_minutes,
    k.drop_rate_pct_avg   > c.drop_rate_pct_warn            as is_drop_rate_breach,
    k.prb_pct_avg         > c.prb_pct_warn                  as is_congested,
    k.latency_ms_avg      > c.latency_ms_warn               as is_latency_breach,
    k.throughput_mbps_avg < c.throughput_mbps_warn          as is_throughput_breach,
    (k.drop_rate_pct_avg > c.drop_rate_pct_warn)::int
      + (k.prb_pct_avg > c.prb_pct_warn)::int
      + (k.latency_ms_avg > c.latency_ms_warn)::int
      + (k.throughput_mbps_avg < c.throughput_mbps_warn)::int
      + iff(zeroifnull(a.major_alarms) > 0, 1, 0)           as breach_count,
    (k.drop_rate_pct_avg > c.drop_rate_pct_crit)
      or (k.prb_pct_avg > c.prb_pct_crit)
      or (k.latency_ms_avg > c.latency_ms_crit)
      or (k.throughput_mbps_avg < c.throughput_mbps_crit)  as is_degraded
from kpi k
left join alarms a on a.cell_id = k.cell_id and a.event_date = k.event_date
left join {{ ref('dim_cell') }} c on c.cell_id = k.cell_id
