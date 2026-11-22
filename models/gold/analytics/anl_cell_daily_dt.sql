{{ config(
    materialized='dynamic_table',
    target_lag='downstream',
    snowflake_warehouse=target.warehouse,
    refresh_mode='auto',
    tags=['gold', 'analytics', 'network', 'kpi', 'm31']
) }}
-- Near-real-time cell day KPIs as a Snowflake DYNAMIC TABLE over the hourly silver roll-up
-- (refreshes incrementally from slv_cell_kpi_hourly, target lag 1 hour).
-- Ported from x_m31_dt_cell_hourly + x_m31_dt_cell_daily (chained dynamic tables on 15-min PM data).
-- Grain: cell_id x event_date.
-- Sources: slv_cell_kpi_hourly, slv_cell_site.
select
    h.cell_id,
    h.event_date,
    c.region_id,
    c.radio_type,
    count(*)                                                as hours_reported,
    sum(h.pm_intervals)                                     as pm_intervals,
    avg(h.prb_pct_avg)                                      as prb_pct,
    max(h.prb_pct_max)                                      as prb_pct_peak,
    avg(h.throughput_mbps_avg)                              as throughput_mbps,
    max(h.drop_rate_pct_avg)                                as worst_hour_drop_rate_pct,
    avg(h.latency_ms_avg)                                   as latency_ms,
    max(h.active_users_max)                                 as peak_users,
    count_if(h.prb_pct_avg > 80)                            as congested_hours
from {{ ref('slv_cell_kpi_hourly') }} h
join {{ ref('slv_cell_site') }} c on c.cell_id = h.cell_id
group by 1, 2, 3, 4
-- Dynamic table with target_lag = downstream: refreshed by the nightly dbt build (OPS.T_DBT_BUILD), not on a timer.
