{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='received_date',
    on_schema_change='append_new_columns',
    cluster_by=['received_date'],
    tags=['gold', 'aggregates', 'usage', 'mediation', 'data_quality', 'e29']
) }}
-- Mediation (xDR) feed health per receive day x source system x parse status: volumes, files,
-- failure rate and how much of the feed made it into silver (slv_xdr_session) with its charge value.
-- Ported from x_e29_xdr_parse_status_daily (PIPE_X.SP_X_E29_XDR_PARSE_STATUS_DAILY).
-- Grain   : received_date x source_system x parse_status
-- Sources : usage.xdr_mediation_raw (bronze - FAILED records never reach silver), slv_xdr_session
with raw as (
    select
        x.record_id, x.source_system, x.parse_status, x.file_name,
        {{ utc_to_local(tz_to_utc('x.received_at')) }}::date as received_date,
        x.payload:record_type::string                       as record_type
    from {{ source('usage', 'xdr_mediation_raw') }} x
    where x.received_at >= dateadd('day', -1, {{ f1_lower_bound('received_date') }})::timestamp_ltz
),
parsed as (
    select
        {{ utc_to_local('received_at_utc') }}::date         as received_date,
        source_system, parse_status,
        count(*)                                            as silver_sessions,
        sum(charge_php)                                     as silver_charge_php,
        round(sum(bytes_down + bytes_up) / power(1024, 3), 3) as silver_gb,
        median(datediff('minute', session_start_utc, received_at_utc)) as median_mediation_lag_min,
        approx_percentile(datediff('minute', session_start_utc, received_at_utc), 0.95) as p95_mediation_lag_min
    from {{ ref('slv_xdr_session') }}
    where received_at_utc >= dateadd('day', -1, {{ f1_lower_bound('received_date') }})::timestamp_ntz
    group by 1, 2, 3
),
daily as (
    select
        received_date, source_system, parse_status,
        count(*)                                            as records,
        approx_count_distinct(file_name)                    as files_approx,
        count_if(record_type is null)                       as records_without_type,
        array_agg(distinct record_type) within group (order by record_type) as record_types
    from raw
    group by 1, 2, 3
)
select
    d.*,
    zeroifnull(p.silver_sessions)                           as silver_sessions,
    zeroifnull(p.silver_charge_php)                         as silver_charge_php,
    zeroifnull(p.silver_gb)                                 as silver_gb,
    p.median_mediation_lag_min,
    p.p95_mediation_lag_min,
    round(ratio_to_report(d.records) over (partition by d.received_date, d.source_system) * 100, 3) as pct_of_source_day,
    round(div0(zeroifnull(p.silver_sessions), d.records) * 100, 2) as silver_coverage_pct,
    current_timestamp()                                     as loaded_at
from daily d
left join parsed p
       on p.received_date = d.received_date and p.source_system = d.source_system and p.parse_status = d.parse_status
where d.received_date >= {{ f1_lower_bound('received_date') }}
