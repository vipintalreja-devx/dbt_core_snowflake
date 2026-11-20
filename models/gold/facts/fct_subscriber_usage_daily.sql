{{ config(
    tags=['gold', 'facts', 'fact'],
    schema='gold',
    materialized='incremental',
    unique_key='event_date',
    incremental_strategy='delete+insert',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns'
) }}
-- Grain: one row per line per local business day with any network usage.
with voice as (
    select subscription_id, event_date,
           count(*)                                         as voice_calls,
           count_if(call_type = 'MO')                       as voice_calls_mo,
           count_if(call_type = 'MT')                       as voice_calls_mt,
           sum(duration_sec) / 60                           as voice_min,
           sum(iff(call_type = 'MO', duration_sec, 0)) / 60 as voice_min_mo,
           sum(iff(is_peak, duration_sec, 0)) / 60          as voice_min_peak,
           count_if(is_roaming)                             as voice_calls_roaming,
           sum(charge_php)                                  as voice_charge_php
    from {{ ref('slv_cdr_voice') }}
    where {{ incremental_date_filter('event_date') }}
    group by 1, 2
),
sms as (
    select subscription_id, event_date,
           count(*)                                         as sms_count,
           count_if(sms_type = 'P2P')                       as sms_p2p,
           count_if(sms_type = 'A2P')                       as sms_a2p,
           sum(charge_php)                                  as sms_charge_php
    from {{ ref('slv_cdr_sms') }}
    where {{ incremental_date_filter('event_date') }}
    group by 1, 2
),
data as (
    select subscription_id, event_date,
           count(*)                                         as data_sessions,
           sum(data_mb)                                     as data_mb,
           sum(iff(rat = '5G', data_mb, 0))                 as data_mb_5g,
           sum(iff(rat = '4G', data_mb, 0))                 as data_mb_4g,
           sum(iff(is_roaming, data_mb, 0))                 as data_mb_roaming,
           count(distinct cell_id)                          as data_cells_used
    from {{ ref('slv_cdr_data') }}
    where {{ incremental_date_filter('event_date') }}
    group by 1, 2
),
keys as (
    select subscription_id, event_date from voice
    union select subscription_id, event_date from sms
    union select subscription_id, event_date from data
)
select
    k.subscription_id,
    k.event_date,
    zeroifnull(v.voice_calls)                               as voice_calls,
    zeroifnull(v.voice_calls_mo)                            as voice_calls_mo,
    zeroifnull(v.voice_calls_mt)                            as voice_calls_mt,
    round(zeroifnull(v.voice_min), 2)                       as voice_min,
    round(zeroifnull(v.voice_min_mo), 2)                    as voice_min_mo,
    round(zeroifnull(v.voice_min_peak), 2)                  as voice_min_peak,
    zeroifnull(v.voice_calls_roaming)                       as voice_calls_roaming,
    zeroifnull(s.sms_count)                                 as sms_count,
    zeroifnull(s.sms_p2p)                                   as sms_p2p,
    zeroifnull(s.sms_a2p)                                   as sms_a2p,
    zeroifnull(d.data_sessions)                             as data_sessions,
    round(zeroifnull(d.data_mb), 3)                         as data_mb,
    round(zeroifnull(d.data_mb_5g), 3)                      as data_mb_5g,
    round(zeroifnull(d.data_mb_4g), 3)                      as data_mb_4g,
    round(zeroifnull(d.data_mb_roaming), 3)                 as data_mb_roaming,
    zeroifnull(d.data_cells_used)                           as data_cells_used,
    round(zeroifnull(v.voice_charge_php) + zeroifnull(s.sms_charge_php), 4) as usage_charge_php
from keys k
left join voice v on v.subscription_id = k.subscription_id and v.event_date = k.event_date
left join sms   s on s.subscription_id = k.subscription_id and s.event_date = k.event_date
left join data  d on d.subscription_id = k.subscription_id and d.event_date = k.event_date
