{{ config(
    tags=['silver', 'usage'],
    schema='silver',
    materialized='incremental',
    unique_key='record_id',
    incremental_strategy='merge',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns'
) }}
-- Parsed mediation xDRs (multi-vendor JSON). FAILED records are excluded here and
-- counted in gold data-quality monitoring; PARTIAL records are kept and flagged.
with src as (
    select *
    from {{ source('usage', 'xdr_mediation_raw') }}
    where received_at >= {{ incremental_ts_lower_bound() }}::timestamp_ltz
      and parse_status in ('OK', 'PARTIAL')
    qualify row_number() over (partition by record_id order by received_at desc) = 1
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
        {{ tz_to_utc('received_at') }}                      as received_at_utc
    from src
)
select
    p.*,
    {{ utc_to_local('p.session_start_utc') }}               as session_start_local,
    {{ utc_to_local('p.session_start_utc') }}::date         as event_date
from parsed p
where p.session_start_utc is not null
  and {{ incremental_date_filter(utc_to_local('p.session_start_utc') ~ '::date', 'event_date') }}
