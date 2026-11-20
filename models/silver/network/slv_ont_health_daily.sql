{{ config(
    tags=['silver', 'network'],
    schema='silver',
    materialized='incremental',
    unique_key='event_date',
    incremental_strategy='delete+insert',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns'
) }}
-- FTTH ONT optical telemetry (400M rows) rolled up to ONT x local day.
-- Rx power below -27 dBm is outside GPON class B+ budget => degraded.
with t as (
    select
        ont_serial,
        {{ utc_to_local(utc_ntz('polled_at')) }}::date      as event_date,
        optical_rx_dbm,
        optical_tx_dbm,
        temperature_c,
        uptime_sec
    from {{ source('network', 'ont_telemetry') }}
    where polled_at >= {{ incremental_ts_lower_bound() }}
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
    min(optical_rx_dbm) < -27                               as is_degraded
from t
where {{ incremental_date_filter('event_date') }}
group by 1, 2
