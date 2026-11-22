{{ config(
    materialized='incremental',
    schema='gold',
    tags=['insights', 'p08'],
    incremental_strategy='delete+insert',
    unique_key='month_start',
    on_schema_change='append_new_columns',
    cluster_by=['month_start'],
    transient=false
) }}
-- P08 cell site ROI | step 3: monthly economics and load of every cell.
-- Grain   : cell_id x month_start
-- Sources : int_p08_cell_traffic_daily, int_p08_cell_revenue_monthly, fct_cell_kpi_daily, dim_cell
-- Inputs are read from one month before the window so month-over-month growth (LAG) is correct
-- for the first restated month; the output keeps only the window.
{% set lb = f3_month_lower_bound('month_start') %}
with traffic as (
    select cell_id, date_trunc('month', event_date)::date   as month_start,
           count(*)                                         as traffic_days,
           sum(voice_min)                                   as voice_min,
           sum(voice_min_peak)                              as voice_min_peak,
           sum(data_mb)                                     as data_mb,
           sum(data_mb_5g)                                  as data_mb_5g,
           sum(sms_count)                                   as sms_count,
           max(max_lines_any_service)                       as peak_daily_lines,
           sum(usage_charge_php)                            as usage_charge_php
    from {{ ref('int_p08_cell_traffic_daily') }}
    where event_date >= dateadd('month', -1, {{ lb }})
    group by 1, 2
),
kpi as (
    select cell_id, date_trunc('month', event_date)::date   as month_start,
           count(*)                                         as kpi_days,
           avg(prb_pct_avg)                                 as prb_pct_avg,
           approx_percentile(prb_pct_peak, 0.95)            as prb_pct_p95,
           max(prb_pct_peak)                                as prb_pct_max,
           avg(throughput_mbps_avg)                         as throughput_mbps_avg,
           avg(drop_rate_pct_avg)                           as drop_rate_pct_avg,
           count_if(is_congested)                           as congested_days,
           count_if(is_degraded)                            as degraded_days,
           sum(alarms)                                      as alarms,
           sum(major_alarms)                                as major_alarms,
           sum(alarm_minutes)                               as alarm_minutes,
           mode(busy_hour)                                  as typical_busy_hour
    from {{ ref('fct_cell_kpi_daily') }}
    where event_date >= dateadd('month', -1, {{ lb }})
    group by 1, 2
),
rev as (
    select home_cell_id as cell_id, month_start, revenue_lines, revenue_php, arpu_php,
           plan_revenue_php, recharge_revenue_php
    from {{ ref('int_p08_cell_revenue_monthly') }}
    where month_start >= dateadd('month', -1, {{ lb }})
),
months as (
    select cell_id, month_start from traffic union
    select cell_id, month_start from kpi union
    select cell_id, month_start from rev
),
joined as (
    select
        m.cell_id,
        m.month_start,
        c.site_name, c.region_id, c.region_name, c.island_group, c.radio_type,
        coalesce(r.revenue_lines, 0)                        as home_lines,
        coalesce(r.revenue_php, 0)                          as home_revenue_php,
        r.arpu_php                                          as home_arpu_php,
        coalesce(t.voice_min, 0)                            as voice_min,
        coalesce(t.data_mb, 0)                              as data_mb,
        round(coalesce(t.data_mb, 0) / 1024, 2)             as data_gb,
        round(coalesce(t.data_mb_5g, 0) / nullif(t.data_mb, 0), 4) as data_5g_share,
        coalesce(t.sms_count, 0)                            as sms_count,
        t.peak_daily_lines,
        k.prb_pct_avg, k.prb_pct_p95, k.prb_pct_max, k.throughput_mbps_avg, k.drop_rate_pct_avg,
        coalesce(k.congested_days, 0)                       as congested_days,
        coalesce(k.degraded_days, 0)                        as degraded_days,
        coalesce(k.alarms, 0)                               as alarms,
        coalesce(k.major_alarms, 0)                         as major_alarms,
        k.typical_busy_hour,
        k.kpi_days
    from months m
    left join traffic t on t.cell_id = m.cell_id and t.month_start = m.month_start
    left join kpi     k on k.cell_id = m.cell_id and k.month_start = m.month_start
    left join rev     r on r.cell_id = m.cell_id and r.month_start = m.month_start
    left join {{ ref('dim_cell') }} c on c.cell_id = m.cell_id
)
select
    j.*,
    round(j.home_revenue_php / nullif(j.data_gb, 0), 2)    as revenue_per_gb_php,
    round(j.home_revenue_php / nullif(j.prb_pct_avg, 0), 2) as revenue_per_prb_point_php,
    round(j.congested_days / nullif(j.kpi_days, 0), 4)     as congested_day_share,
    lag(j.data_gb) over (partition by j.cell_id order by j.month_start)          as data_gb_prev_month,
    round(j.data_gb / nullif(lag(j.data_gb) over (partition by j.cell_id order by j.month_start), 0) - 1, 4) as data_gb_mom_growth,
    round(j.home_revenue_php / nullif(lag(j.home_revenue_php) over (partition by j.cell_id order by j.month_start), 0) - 1, 4) as revenue_mom_growth,
    current_timestamp()                                    as loaded_at
from joined j
qualify j.month_start >= {{ lb }}
