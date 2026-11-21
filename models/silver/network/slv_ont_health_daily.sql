{%- set changed = cdc_source_changed_dates('network', 'ont_telemetry', utc_to_local(utc_ntz('polled_at')) ~ '::date') -%}
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
-- FTTH ONT optical telemetry (400M rows) rolled up to ONT x local day.
-- Rx power below -27 dBm is outside GPON class B+ budget => degraded.
-- CDC: only local days with changed telemetry are recomputed.
with t as (
    select
        ont_serial,
        {{ utc_to_local(utc_ntz('polled_at')) }}::date      as event_date,
        optical_rx_dbm,
        optical_tx_dbm,
        temperature_c,
        uptime_sec
    from {{ source('network', 'ont_telemetry') }}
    {% if changed -%}
    where polled_at >= (select dateadd('day', -1, min(d))::timestamp_ntz from {{ changed }} c(d))
    {%- else -%}
    where polled_at >= {{ incremental_ts_lower_bound() }}
    {%- endif %}
)
select
    ont_serial,
    event_date,
    count(*)                                                as polls,
    min(optical_rx_dbm)                                     as rx_dbm_min,
    avg(optical_rx_dbm)                                     as rx_dbm_avg,
    avg(optical_tx_dbm)                                     as tx_dbm_avg,
    max(temperature_c)                                      as temperature_c_max,
    count_if(optical_rx_dbm < -27)                          as low_rx_polls,
    min(optical_rx_dbm) < -27                               as is_degraded,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from t
where {% if changed %}event_date in {{ changed }}{% else %}{{ incremental_date_filter('event_date') }}{% endif %}
group by 1, 2
