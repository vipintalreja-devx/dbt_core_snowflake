{{ config(
    materialized='incremental',
    schema='silver',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['insights', 'p01', 'experience'],
    post_hook=["{{ ins_retention('event_date') }}"]
) }}
-- P01 Customer experience score, step 1: where did each line actually use the network?
-- Grain  : subscription_id x event_date x cell_id
-- Sources: slv_cdr_voice (calls, minutes per serving cell), slv_cdr_data (sessions, MB per cell)
-- Weight : minutes + MB/50 + 1 per call/session, so a heavy data cell counts more than a passing call.
-- Window : rolling var('insights_daily_days'); incremental runs re-process changed CDR dates only.
with voice as (
    select subscription_id, event_date, cell_id,
           count(*)                                          as calls,
           sum(duration_sec) / 60                            as voice_min,
           0::number                                         as sessions,
           0::float                                          as data_mb
    from {{ ref('slv_cdr_voice') }}
    where event_date >= {{ ins_daily_lower_bound('event_date', ref('slv_cdr_voice'), 'event_date') }}
      and cell_id is not null
    group by 1, 2, 3
),
data as (
    select subscription_id, event_date, cell_id,
           0::number, 0::float,
           count(*)                                          as sessions,
           sum(data_mb)                                      as data_mb
    from {{ ref('slv_cdr_data') }}
    where event_date >= {{ ins_daily_lower_bound('event_date', ref('slv_cdr_voice'), 'event_date') }}
      and cell_id is not null
    group by 1, 2, 3
)
select
    subscription_id,
    event_date,
    cell_id,
    sum(calls)                                               as calls,
    round(sum(voice_min), 2)                                 as voice_min,
    sum(sessions)                                            as data_sessions,
    round(sum(data_mb), 3)                                   as data_mb,
    round(sum(voice_min) + sum(data_mb) / 50 + sum(calls) + sum(sessions), 4) as usage_weight
from (select * from voice union all select * from data)
group by 1, 2, 3
