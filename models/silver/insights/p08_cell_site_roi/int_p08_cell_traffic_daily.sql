{{ config(
    materialized='incremental',
    schema='silver',
    tags=['insights', 'p08'],
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    transient=true
) }}
-- P08 cell site ROI | step 1: traffic carried by each cell per Manila business day.
-- Grain   : cell_id x event_date
-- Sources : slv_cdr_voice, slv_cdr_data, slv_cdr_sms (CDC silver; window prunes on event_date)
with voice as (
    select cell_id, event_date,
           count(*)                                         as voice_calls,
           round(sum(duration_sec) / 60, 1)                 as voice_min,
           round(sum(iff(is_peak, duration_sec, 0)) / 60, 1) as voice_min_peak,
           count(distinct subscription_id)                  as voice_lines,
           count_if(call_type = 'MO')                       as voice_calls_mo,
           sum(charge_php)                                  as voice_charge_php
    from {{ ref('slv_cdr_voice') }}
    where event_date >= {{ f1_lower_bound('event_date') }} and cell_id is not null
    group by 1, 2
),
data as (
    select cell_id, event_date,
           count(*)                                         as data_sessions,
           round(sum(data_mb), 1)                           as data_mb,
           round(sum(iff(rat = '5G', data_mb, 0)), 1)       as data_mb_5g,
           round(sum(iff(rat = '4G', data_mb, 0)), 1)       as data_mb_4g,
           count(distinct subscription_id)                  as data_lines
    from {{ ref('slv_cdr_data') }}
    where event_date >= {{ f1_lower_bound('event_date') }} and cell_id is not null
    group by 1, 2
),
sms as (
    select cell_id, event_date,
           count(*)                                         as sms_count,
           count(distinct subscription_id)                  as sms_lines,
           sum(charge_php)                                  as sms_charge_php
    from {{ ref('slv_cdr_sms') }}
    where event_date >= {{ f1_lower_bound('event_date') }} and cell_id is not null
    group by 1, 2
),
keys as (
    select cell_id, event_date from voice union
    select cell_id, event_date from data union
    select cell_id, event_date from sms
)
select
    k.cell_id,
    k.event_date,
    coalesce(v.voice_calls, 0)                              as voice_calls,
    coalesce(v.voice_calls_mo, 0)                           as voice_calls_mo,
    coalesce(v.voice_min, 0)                                as voice_min,
    coalesce(v.voice_min_peak, 0)                           as voice_min_peak,
    coalesce(v.voice_lines, 0)                              as voice_lines,
    coalesce(d.data_sessions, 0)                            as data_sessions,
    coalesce(d.data_mb, 0)                                  as data_mb,
    coalesce(d.data_mb_5g, 0)                               as data_mb_5g,
    coalesce(d.data_mb_4g, 0)                               as data_mb_4g,
    coalesce(d.data_lines, 0)                               as data_lines,
    coalesce(s.sms_count, 0)                                as sms_count,
    coalesce(s.sms_lines, 0)                                as sms_lines,
    coalesce(v.voice_charge_php, 0) + coalesce(s.sms_charge_php, 0) as usage_charge_php,
    greatest(coalesce(v.voice_lines, 0), coalesce(d.data_lines, 0), coalesce(s.sms_lines, 0)) as max_lines_any_service
from keys k
left join voice v on v.cell_id = k.cell_id and v.event_date = k.event_date
left join data  d on d.cell_id = k.cell_id and d.event_date = k.event_date
left join sms   s on s.cell_id = k.cell_id and s.event_date = k.event_date
