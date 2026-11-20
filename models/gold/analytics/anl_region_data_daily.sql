{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['gold', 'analytics', 'usage', 'data', 'm03']
) }}
-- Mobile-data traffic per serving region and radio technology per day, including the
-- "5G-capable device served on 3G/4G" coverage-gap indicator used by RAN planning.
-- Ported from x_m03_region_data_daily (PIPE_X.SP_X_M03_REGION_DATA_DAILY).
-- Grain: event_date x serving_region_name x rat.
-- Sources: slv_cdr_data, slv_cell_site, int_subscriber_profile, slv_device.
with sessions as (
    select
        d.event_date,
        coalesce(c.region_name, 'UNKNOWN')                  as serving_region_name,
        d.rat,
        d.subscription_id,
        d.cell_id,
        d.data_mb,
        d.duration_sec,
        d.is_roaming,
        coalesce(dv.supports_5g, false)                     as device_5g_capable
    from {{ ref('slv_cdr_data') }} d
    left join {{ ref('slv_cell_site') }} c          on c.cell_id = d.cell_id
    left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = d.subscription_id
    left join {{ ref('slv_device') }} dv            on dv.device_id = p.device_id
    where {{ incremental_date_filter('d.event_date', 'event_date') }}
)
select
    event_date,
    serving_region_name,
    rat,
    count(*)                                                as sessions,
    approx_count_distinct(subscription_id)                  as approx_users,
    count(distinct cell_id)                                 as cells_used,
    round(sum(data_mb) / 1024, 3)                           as data_gb,
    round(avg(data_mb), 3)                                  as avg_session_mb,
    approx_percentile(data_mb, 0.5)                         as p50_session_mb,
    approx_percentile(data_mb, 0.95)                        as p95_session_mb,
    round(div0(sum(data_mb) * 8, nullif(sum(duration_sec), 0)), 4) as avg_throughput_mbit_s,
    round(div0(count_if(is_roaming), count(*)), 4)          as roaming_session_share,
    count_if(device_5g_capable and rat <> '5G')             as capable_5g_sessions_on_legacy,
    round(div0(count_if(device_5g_capable and rat <> '5G'), count(*)), 4) as coverage_gap_5g_share,
    round(ratio_to_report(sum(data_mb)) over (partition by event_date, serving_region_name), 4) as rat_share_in_region,
    current_timestamp()                                     as loaded_at
from sessions
group by 1, 2, 3
