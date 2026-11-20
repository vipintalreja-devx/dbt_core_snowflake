{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['gold', 'aggregates', 'usage', 'voice', 'e17']
) }}
-- Voice traffic per Manila day x call type x roaming flag x serving region: minutes, peak share,
-- short-call ratio (dropped-call proxy) and duration distribution.
-- Ported from x_e17_voice_daily_type (PIPE_X.SP_X_E17_VOICE_DAILY_TYPE).
-- Grain   : event_date x call_type x is_roaming x region_name
-- Sources : slv_cdr_voice, dim_cell, slv_time_slot (peak flag), dim_date
with calls as (
    select
        v.call_id, v.subscription_id, v.event_date, v.call_type, v.is_roaming, v.call_country,
        v.duration_sec, v.charge_php, v.is_peak, v.slot_id,
        coalesce(c.region_name, 'UNKNOWN')                  as region_name,
        c.radio_type
    from {{ ref('slv_cdr_voice') }} v
    left join {{ ref('dim_cell') }} c on c.cell_id = v.cell_id
    where v.event_date >= {{ f1_lower_bound('event_date', 7) }}
),
slot_peak as (
    -- busiest half-hour slot per group
    select event_date, call_type, is_roaming, region_name,
           max_by(slot_id, slot_minutes)                    as busy_slot_id
    from (select event_date, call_type, is_roaming, region_name, slot_id, sum(duration_sec) / 60 as slot_minutes
          from calls group by 1, 2, 3, 4, 5)
    group by 1, 2, 3, 4
),
daily as (
    select
        event_date, call_type, is_roaming, region_name,
        count(*)                                            as calls,
        approx_count_distinct(subscription_id)              as callers_approx,
        round(sum(duration_sec) / 60, 1)                    as minutes,
        round(sum(iff(is_peak, duration_sec, 0)) / 60, 1)   as peak_minutes,
        round(avg(duration_sec), 1)                         as avg_duration_sec,
        median(duration_sec)                                as median_duration_sec,
        approx_percentile(duration_sec, 0.95)               as p95_duration_sec,
        count_if(duration_sec < 10)                         as short_calls,
        count_if(radio_type = '5G' or radio_type = 'NR')    as calls_on_5g_cells,
        sum(charge_php)                                     as charge_php,
        count(distinct iff(is_roaming, call_country, null)) as roaming_countries
    from calls
    group by 1, 2, 3, 4
)
select
    d.*,
    ts.slot_label                                           as busy_slot,
    round(div0(d.peak_minutes, d.minutes) * 100, 2)         as peak_minutes_pct,
    round(div0(d.short_calls, d.calls) * 100, 3)            as short_call_ratio_pct,
    round(div0(d.charge_php, d.minutes), 4)                 as yield_php_per_min,
    round(ratio_to_report(d.minutes) over (partition by d.event_date), 6) as share_of_day_minutes,
    round(avg(d.minutes) over (partition by d.call_type, d.is_roaming, d.region_name order by d.event_date
                               rows between 6 preceding and current row), 1) as minutes_7d_avg,
    dd.is_weekend,
    current_timestamp()                                     as loaded_at
from daily d
left join slot_peak sp
       on sp.event_date = d.event_date and sp.call_type = d.call_type
      and sp.is_roaming = d.is_roaming and sp.region_name = d.region_name
left join {{ ref('slv_time_slot') }} ts on ts.slot_id = sp.busy_slot_id
left join {{ ref('dim_date') }} dd      on dd.date_day = d.event_date
qualify d.event_date >= {{ f1_lower_bound('event_date') }}
