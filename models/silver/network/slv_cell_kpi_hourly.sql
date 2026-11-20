{{ config(
    tags=['silver', 'network'],
    schema='silver',
    materialized='incremental',
    unique_key='event_date',
    incremental_strategy='delete+insert',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns'
) }}
-- 15-minute PM counters (315M rows, NTZ in UTC) rolled up to cell x local hour.
-- NaN latency samples are treated as missing; drop rate converted fraction -> percent.
with pm as (
    select
        cell_id,
        {{ utc_to_local(utc_ntz('interval_start')) }}       as interval_local,
        prb_utilization_pct,
        avg_throughput_mbps,
        iff(latency_ms = 'NaN'::float or latency_ms < 0, null, latency_ms) as latency_ms,
        drop_call_rate * 100                                as drop_rate_pct,
        active_users
    from {{ source('network', 'cell_performance_15min') }}
    where interval_start >= {{ incremental_ts_lower_bound() }}
    qualify row_number() over (partition by cell_id, interval_start order by active_users desc) = 1
)
select
    cell_id,
    date_trunc('hour', interval_local)                      as hour_local,
    date_trunc('hour', interval_local)::date                as event_date,
    count(*)                                                as pm_intervals,
    avg(prb_utilization_pct)                                as prb_pct_avg,
    max(prb_utilization_pct)                                as prb_pct_max,
    avg(avg_throughput_mbps)                                as throughput_mbps_avg,
    avg(latency_ms)                                         as latency_ms_avg,
    max(latency_ms)                                         as latency_ms_max,
    avg(drop_rate_pct)                                      as drop_rate_pct_avg,
    max(active_users)                                       as active_users_max
from pm
where {{ incremental_date_filter('interval_local::date', 'event_date') }}
group by 1, 2, 3
