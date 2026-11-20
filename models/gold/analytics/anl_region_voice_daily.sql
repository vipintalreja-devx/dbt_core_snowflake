{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['gold', 'analytics', 'usage', 'voice', 'm02']
) }}
-- Voice traffic per serving region and Manila business day: volume, callers, duration distribution,
-- peak-hour share, roaming / international share and "away from home region" mobility share.
-- Ported from x_m02_region_voice_daily (PIPE_X.SP_X_M02_REGION_VOICE_DAILY).
-- Grain: event_date x serving_region_name.
-- Sources: slv_cdr_voice, slv_cell_site, int_subscriber_profile, slv_time_slot.
with calls as (
    select
        v.event_date,
        v.subscription_id,
        v.duration_sec,
        v.charge_php,
        v.call_type,
        v.is_roaming,
        v.call_country,
        v.is_peak,
        coalesce(c.region_name, 'UNKNOWN')                  as serving_region_name,
        p.home_region_name,
        p.plan_category
    from {{ ref('slv_cdr_voice') }} v
    left join {{ ref('slv_cell_site') }} c          on c.cell_id = v.cell_id
    left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = v.subscription_id
    where {{ incremental_date_filter('v.event_date', 'event_date') }}
)
select
    event_date,
    serving_region_name,
    count(*)                                                as calls,
    count(distinct subscription_id)                         as callers,
    count_if(call_type = 'MO')                              as calls_mo,
    count_if(call_type = 'MT')                              as calls_mt,
    count_if(call_type = 'FWD')                             as calls_forwarded,
    round(sum(duration_sec) / 60, 1)                        as minutes,
    round(avg(duration_sec), 1)                             as avg_call_sec,
    median(duration_sec)                                    as median_call_sec,
    approx_percentile(duration_sec, 0.9)                    as p90_call_sec,
    count_if(duration_sec < 10)                             as short_calls_lt_10s,
    round(div0(count_if(duration_sec < 10), count(*)), 4)   as short_call_ratio,
    round(div0(count_if(is_peak), count(*)), 4)             as peak_share,
    round(div0(count_if(is_roaming), count(*)), 4)          as roaming_share,
    count(distinct iff(call_country <> 'PH', call_country, null)) as foreign_countries,
    round(div0(count_if(home_region_name is not null and home_region_name <> serving_region_name), count(*)), 4) as away_from_home_share,
    round(div0(count_if(plan_category = 'POSTPAID'), count(*)), 4) as postpaid_share,
    round(sum(charge_php), 2)                               as charge_php,
    round(ratio_to_report(count(*)) over (partition by event_date), 5) as region_traffic_share,
    current_timestamp()                                     as loaded_at
from calls
group by 1, 2
