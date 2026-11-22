{{ config(
    materialized='incremental',
    schema='silver',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['insights', 'p01', 'experience'],
    post_hook=["{{ ins_retention('event_date') }}"]
) }}
-- P01 step 2: a 0-100 quality score per cell and day.
-- Grain  : cell_id x event_date
-- Sources: fct_cell_kpi_daily (PM KPIs + breach flags vs the cell's thresholds), dim_cell (region),
--          slv_alarm_event (critical alarm minutes), fct_outage (regional outage minutes that day)
-- Score  : 100 minus penalties: drop-rate breach 30, congestion 20, latency 15, throughput 15,
--          critical alarm minutes up to 10, regional outage minutes up to 10; incomplete PM caps at 80.
with kpi as (
    select k.*, c.region_id, c.region_name
    from {{ ref('fct_cell_kpi_daily') }} k
    join {{ ref('dim_cell') }} c on c.cell_id = k.cell_id
    where k.event_date >= {{ ins_daily_lower_bound('event_date', ref('fct_cell_kpi_daily'), 'event_date') }}
),
crit as (
    select cell_id, event_date, sum(coalesce(duration_min, 0)) as critical_alarm_min
    from {{ ref('slv_alarm_event') }}
    where severity in ('CRITICAL', 'MAJOR')
      and event_date >= {{ ins_daily_lower_bound('event_date', ref('fct_cell_kpi_daily'), 'event_date') }}
    group by 1, 2
),
outage as (
    select region_id, event_date, sum(coalesce(outage_minutes, 0)) as region_outage_min
    from {{ ref('fct_outage') }}
    where event_date >= {{ ins_daily_lower_bound('event_date', ref('fct_cell_kpi_daily'), 'event_date') }}
    group by 1, 2
)
select
    k.cell_id,
    k.event_date,
    k.region_id,
    k.region_name,
    k.drop_rate_pct_avg,
    k.prb_pct_avg,
    k.latency_ms_avg,
    k.throughput_mbps_avg,
    k.pm_completeness,
    coalesce(cr.critical_alarm_min, 0)                       as critical_alarm_min,
    coalesce(o.region_outage_min, 0)                         as region_outage_min,
    least(
        iff(coalesce(k.pm_completeness, 1) < 0.9, 80, 100),
        greatest(0,
            100
            - iff(k.is_drop_rate_breach, 30, 0)
            - iff(k.is_congested, 20, 0)
            - iff(k.is_latency_breach, 15, 0)
            - iff(k.is_throughput_breach, 15, 0)
            - least(10, coalesce(cr.critical_alarm_min, 0) / 30)
            - least(10, coalesce(o.region_outage_min, 0) / 60)
        )
    )::number(6, 2)                                          as cell_quality_score
from kpi k
left join crit cr on cr.cell_id = k.cell_id and cr.event_date = k.event_date
left join outage o on o.region_id = k.region_id and o.event_date = k.event_date
