{{ config(
    tags=['silver', 'usage', 'cdc'],
    schema='silver',
    materialized='incremental',
    unique_key='record_id',
    incremental_strategy='merge',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('event_date') }}",
               "{{ cdc_log_deletes('record_id', 'event_date') }}",
               "{{ cdc_purge_tombstones() }}"]
) }}
-- Parsed mediation xDRs (multi-vendor JSON). FAILED records are excluded here and
-- counted in gold data-quality monitoring; PARTIAL records are kept and flagged.
-- Bronze XDR_MEDIATION_RAW is TRANSIENT (1-day retention): CDC is used while the
-- watermark is < 20h old, otherwise the received_at lookback window applies.
with src as (
    select *
    from {{ cdc_source('usage', 'xdr_mediation_raw', "received_at >= " ~ incremental_ts_lower_bound() ~ "::timestamp_ltz", retention_hours=20) }}
    where parse_status in ('OK', 'PARTIAL')
    qualify row_number() over (partition by record_id order by iff(_cdc_action = 'DELETE', 1, 0) desc, received_at desc) = 1
),
parsed as (
    select
        record_id,
        source_system,
        file_name,
        parse_status,
        parse_status = 'PARTIAL'                            as is_partial,
        payload:record_type::string                         as record_type,
        payload:sub_id::number                              as subscription_id,
        {{ normalize_msisdn('payload:msisdn::string') }}    as msisdn,
        payload:imsi::string                                as imsi,
        try_to_number(payload:cell.id::string)              as cell_id,
        payload:cell.lac::number                            as lac,
        payload:cell.rat::string                            as rat,
        to_timestamp_ntz(payload:start_epoch_ms::number, 3) as session_start_utc,
        payload:duration_ms::number                         as duration_ms,
        payload:bytes.down::number                          as bytes_down,
        payload:bytes.up::number                            as bytes_up,
        payload:charging.amount_php::number(18, 6)          as charge_php,
        payload:charging.rating_group::number               as rating_group,
        payload:charging.promo_code::string                 as promo_code,
        payload:qos.qci::number                             as qci,
        payload:qos.arp::number                             as arp,
        coalesce(array_size(payload:events), 0)             as event_count,
        payload:events                                      as events,
        {{ tz_to_utc('received_at') }}                      as received_at_utc,
        _cdc_action
    from src
)
select
    p.* exclude (_cdc_action),
    {{ utc_to_local('p.session_start_utc') }}               as session_start_local,
    {{ utc_to_local('p.session_start_utc') }}::date         as event_date,
    p._cdc_action,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from parsed p
where p.session_start_utc is not null
  and {{ full_refresh_floor(utc_to_local('p.session_start_utc') ~ '::date') }}
