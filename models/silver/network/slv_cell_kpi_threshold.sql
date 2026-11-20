{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'network'],
    transient=true
) }}
-- Per-cell KPI thresholds, pivoted. DROP_CALL_RATE thresholds are in PERCENT while the
-- PM counter is a FRACTION - silver keeps both on the PERCENT scale.
select
    cell_id,
    max(iff(kpi_name = 'DROP_CALL_RATE',  warning_threshold,  null)) as drop_rate_pct_warn,
    max(iff(kpi_name = 'DROP_CALL_RATE',  critical_threshold, null)) as drop_rate_pct_crit,
    max(iff(kpi_name = 'PRB_UTILIZATION', warning_threshold,  null)) as prb_pct_warn,
    max(iff(kpi_name = 'PRB_UTILIZATION', critical_threshold, null)) as prb_pct_crit,
    max(iff(kpi_name = 'LATENCY',         warning_threshold,  null)) as latency_ms_warn,
    max(iff(kpi_name = 'LATENCY',         critical_threshold, null)) as latency_ms_crit,
    max(iff(kpi_name = 'AVG_THROUGHPUT',  warning_threshold,  null)) as throughput_mbps_warn,
    max(iff(kpi_name = 'AVG_THROUGHPUT',  critical_threshold, null)) as throughput_mbps_crit
from {{ source('network', 'cell_kpi_threshold') }}
where coalesce(is_enabled, true)
group by cell_id
