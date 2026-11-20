{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['gold', 'analytics', 'usage', 'sms', 'm26']
) }}
-- SMS traffic per Manila hour, SMS type and roaming flag, using TIME_SLICE buckets. Includes A2P
-- (enterprise) revenue, distinct senders, serving-region spread and hour-over-day share.
-- Ported from x_m26_stream_sms_hourly (PIPE_X.SP_X_M26_STREAM_SMS_HOURLY): the stream + task + MERGE
-- mechanics are replaced by the incremental silver feed; this keeps the business output.
-- Grain: hour_local x sms_type x is_roaming.
-- Sources: slv_cdr_sms, slv_cell_site, int_subscriber_profile.
with sms as (
    select
        s.event_date,
        time_slice(s.sent_at_local, 1, 'HOUR', 'START')     as hour_local,
        s.sms_type,
        s.is_roaming,
        s.subscription_id,
        s.sender_msisdn,
        s.charge_php,
        c.region_id,
        p.plan_category
    from {{ ref('slv_cdr_sms') }} s
    left join {{ ref('slv_cell_site') }} c          on c.cell_id = s.cell_id
    left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = s.subscription_id
    where {{ incremental_date_filter('s.event_date', 'event_date') }}
)
select
    hour_local,
    event_date,
    hour(hour_local)                                        as hour_of_day,
    sms_type,
    is_roaming,
    count(*)                                                as sms,
    approx_count_distinct(subscription_id)                  as approx_lines,
    approx_count_distinct(sender_msisdn)                    as approx_senders,
    count(distinct region_id)                               as regions,
    count_if(plan_category = 'POSTPAID')                    as sms_postpaid,
    round(sum(charge_php), 4)                               as charge_php,
    round(ratio_to_report(count(*)) over (partition by event_date, sms_type), 5) as share_of_day_for_type,
    current_timestamp()                                     as loaded_at
from sms
group by 1, 2, 3, 4, 5
