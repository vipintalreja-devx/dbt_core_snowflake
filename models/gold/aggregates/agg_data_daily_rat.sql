{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['gold', 'aggregates', 'usage', 'data', 'e18']
) }}
-- Mobile data per Manila day x radio access technology x APN x serving region, incl. 5G-device
-- users stuck on legacy RATs (network upgrade opportunity).
-- Ported from x_e18_data_daily_rat (PIPE_X.SP_X_E18_DATA_DAILY_RAT).
-- Grain   : event_date x rat x apn x region_name
-- Sources : slv_cdr_data, dim_cell, int_subscriber_profile (device 5G capability)
with sess as (
    select
        d.session_id, d.subscription_id, d.event_date, d.rat, coalesce(d.apn, 'UNKNOWN') as apn,
        d.data_mb, d.duration_sec, d.is_roaming,
        coalesce(c.region_name, 'UNKNOWN')                  as region_name,
        coalesce(p.device_supports_5g, false)               as device_supports_5g
    from {{ ref('slv_cdr_data') }} d
    left join {{ ref('dim_cell') }} c               on c.cell_id = d.cell_id
    left join {{ ref('int_subscriber_profile') }} p  on p.subscription_id = d.subscription_id
    where d.event_date >= {{ f1_lower_bound('event_date', 7) }}
),
daily as (
    select
        event_date, rat, apn, region_name,
        count(*)                                            as sessions,
        approx_count_distinct(subscription_id)              as users_approx,
        round(sum(data_mb) / 1024, 3)                       as data_gb,
        median(data_mb)                                     as median_session_mb,
        approx_percentile(data_mb, 0.95)                    as p95_session_mb,
        round(avg(duration_sec), 0)                         as avg_session_sec,
        round(div0(sum(data_mb) * 8, sum(duration_sec)), 3) as avg_throughput_mbps,
        count_if(is_roaming)                                as roaming_sessions,
        count_if(device_supports_5g and rat <> '5G')        as sessions_5g_device_on_legacy_rat,
        round(sum(iff(device_supports_5g and rat <> '5G', data_mb, 0)) / 1024, 3) as gb_5g_device_on_legacy_rat
    from sess
    group by 1, 2, 3, 4
)
select
    d.*,
    round(div0(d.data_gb * 1024, d.users_approx), 1)        as mb_per_user,
    round(ratio_to_report(d.data_gb) over (partition by d.event_date, d.region_name), 6) as rat_apn_share_of_region_gb,
    round(avg(d.data_gb) over (partition by d.rat, d.apn, d.region_name order by d.event_date
                               rows between 6 preceding and current row), 3) as data_gb_7d_avg,
    round(div0(d.data_gb - lag(d.data_gb) over (partition by d.rat, d.apn, d.region_name order by d.event_date),
               lag(d.data_gb) over (partition by d.rat, d.apn, d.region_name order by d.event_date)) * 100, 2) as data_gb_dod_pct,
    current_timestamp()                                     as loaded_at
from daily d
qualify d.event_date >= {{ f1_lower_bound('event_date') }}
