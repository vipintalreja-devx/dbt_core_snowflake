{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['gold', 'aggregates', 'usage', 'sms', 'e16']
) }}
-- SMS traffic per Manila day x SMS type x roaming flag x serving region, with busy hour.
-- Ported from x_e16_sms_daily_type (PIPE_X.SP_X_E16_SMS_DAILY_TYPE).
-- Grain   : event_date x sms_type x is_roaming x region_name
-- Sources : slv_cdr_sms, dim_cell (serving cell -> region), int_subscriber_profile (MVNO brand)
with sms as (
    select
        s.sms_id, s.subscription_id, s.event_date, s.sent_at_local, s.sms_type, s.is_roaming, s.charge_php,
        coalesce(c.region_name, 'UNKNOWN')                  as region_name,
        coalesce(p.is_mvno_line, false)                     as is_mvno_line
    from {{ ref('slv_cdr_sms') }} s
    left join {{ ref('dim_cell') }} c                on c.cell_id = s.cell_id
    left join {{ ref('int_subscriber_profile') }} p   on p.subscription_id = s.subscription_id
    where s.event_date >= {{ f1_lower_bound('event_date', 7) }}
),
hourly as (
    select event_date, sms_type, is_roaming, region_name,
           time_slice(sent_at_local, 1, 'HOUR')             as hour_slot,
           count(*)                                         as hour_sms
    from sms group by 1, 2, 3, 4, 5
),
busy as (
    select event_date, sms_type, is_roaming, region_name,
           hour(max_by(hour_slot, hour_sms))                as busy_hour,
           max(hour_sms)                                    as busy_hour_sms
    from hourly group by 1, 2, 3, 4
),
daily as (
    select
        event_date, sms_type, is_roaming, region_name,
        count(*)                                            as sms,
        approx_count_distinct(subscription_id)              as senders_approx,
        sum(charge_php)                                     as charge_php,
        count_if(charge_php = 0)                            as zero_rated_sms,
        count_if(is_mvno_line)                              as mvno_sms
    from sms
    group by 1, 2, 3, 4
)
select
    d.*,
    b.busy_hour,
    b.busy_hour_sms,
    round(div0(b.busy_hour_sms, d.sms) * 100, 2)            as busy_hour_share_pct,
    round(ratio_to_report(d.sms) over (partition by d.event_date, d.region_name), 6) as type_share_of_region_day,
    round(avg(d.sms) over (partition by d.sms_type, d.is_roaming, d.region_name order by d.event_date
                           rows between 6 preceding and current row), 1) as sms_7d_avg,
    round(div0(d.sms - lag(d.sms) over (partition by d.sms_type, d.is_roaming, d.region_name order by d.event_date),
               lag(d.sms) over (partition by d.sms_type, d.is_roaming, d.region_name order by d.event_date)) * 100, 2) as sms_dod_pct,
    current_timestamp()                                     as loaded_at
from daily d
left join busy b
       on b.event_date = d.event_date and b.sms_type = d.sms_type
      and b.is_roaming = d.is_roaming and b.region_name = d.region_name
qualify d.event_date >= {{ f1_lower_bound('event_date') }}
