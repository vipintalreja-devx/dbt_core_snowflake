{%- set changed = cdc_source_changed_dates('network', 'cell_performance_15min', utc_to_local(utc_ntz('interval_start')) ~ '::date') -%}
{{ config(
    tags=['silver', 'network', 'cdc'],
    schema='silver',
    materialized='incremental',
    unique_key='event_date',
    incremental_strategy='delete+insert',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('event_date', has_action=false) }}"]
) }}
-- 15-minute PM counters (315M rows, NTZ in UTC) rolled up to cell x local hour.
-- NaN latency samples are treated as missing; drop rate converted fraction -> percent.
-- CDC: only local days with changed PM rows are recomputed.
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
    {% if changed -%}
    where interval_start >= (select dateadd('day', -1, min(d))::timestamp_ntz from {{ changed }} c(d))
    {%- else -%}
    where interval_start >= {{ incremental_ts_lower_bound() }}
    {%- endif %}
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
    max(active_users)                                       as active_users_max,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from pm
where {% if changed %}interval_local::date in {{ changed }}{% else %}{{ incremental_date_filter('interval_local::date', 'event_date') }}{% endif %}
group by 1, 2, 3
