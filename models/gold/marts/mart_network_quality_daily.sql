{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'marts', 'kpi'],
    transient=false,
    copy_grants=true
) }}
-- Regional network quality scorecard (replaces PIPE.SP_GOLD_CELL_DAILY_QUALITY roll-ups).
with outages as (
    select region_id, event_date, count(*) as outages, sum(outage_minutes) as outage_minutes,
           sum(impacted_lines) as impacted_lines
    from {{ ref('fct_outage') }}
    group by 1, 2
)
select
    k.event_date,
    c.region_id,
    c.region_name,
    count(*)                                                as cells_reporting,
    count_if(k.is_degraded)                                 as cells_degraded,
    count_if(k.is_congested)                                as cells_congested,
    round(div0(count_if(k.is_degraded), count(*)) * 100, 2) as pct_cells_degraded,
    round(avg(k.drop_rate_pct_avg), 4)                      as drop_rate_pct_avg,
    round(avg(k.prb_pct_avg), 2)                            as prb_pct_avg,
    round(avg(k.throughput_mbps_avg), 2)                    as throughput_mbps_avg,
    round(avg(k.latency_ms_avg), 2)                         as latency_ms_avg,
    round(avg(k.pm_completeness) * 100, 2)                  as pm_completeness_pct,
    sum(k.major_alarms)                                     as major_alarms,
    zeroifnull(max(o.outages))                              as outages,
    zeroifnull(max(o.outage_minutes))                       as outage_minutes,
    zeroifnull(max(o.impacted_lines))                       as outage_impacted_lines
from {{ ref('fct_cell_kpi_daily') }} k
join {{ ref('dim_cell') }} c on c.cell_id = k.cell_id
left join outages o on o.region_id = c.region_id and o.event_date = k.event_date
group by 1, 2, 3
