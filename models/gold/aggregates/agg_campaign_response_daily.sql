{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['gold', 'aggregates', 'marketing', 'e10']
) }}
-- Campaign sends, opens and conversions per send day x campaign, with audience mix.
-- Ported from x_e10_campaign_response_daily (PIPE_X.SP_X_E10_CAMPAIGN_RESPONSE_DAILY).
-- Grain   : event_date x campaign_id
-- Sources : slv_campaign_response, slv_campaign, int_subscriber_profile
with resp as (
    select
        r.response_id, r.campaign_id, r.subscription_id, r.event_date, r.is_opened, r.is_converted, r.hours_to_open,
        coalesce(p.island_group, 'UNKNOWN')                 as island_group,
        coalesce(p.churn_band, 'UNSCORED')                  as churn_band,
        coalesce(p.segment_code, 'NONE')                    as segment_code,
        coalesce(p.has_marketing_consent, false)            as has_marketing_consent
    from {{ ref('slv_campaign_response') }} r
    left join (
        select ip.subscription_id, ip.island_group, ip.churn_band, ip.segment_code, dc.has_marketing_consent
        from {{ ref('int_subscriber_profile') }} ip
        left join {{ ref('dim_customer') }} dc on dc.customer_id = ip.customer_id
    ) p on p.subscription_id = r.subscription_id
    where r.event_date >= {{ f1_lower_bound('event_date') }}
),
seg as (
    select event_date, campaign_id, segment_code, count(*) as sent
    from resp group by 1, 2, 3
),
seg_obj as (
    select event_date, campaign_id, object_agg(segment_code, sent::variant) as sent_by_segment
    from seg group by 1, 2
),
daily as (
    select
        event_date, campaign_id,
        count(*)                                            as sent,
        approx_count_distinct(subscription_id)              as recipients_approx,
        count_if(is_opened)                                 as opened,
        count_if(is_converted)                              as converted,
        median(hours_to_open)                               as median_hours_to_open,
        count_if(churn_band = 'HIGH')                       as sent_high_churn,
        count_if(churn_band = 'HIGH' and is_converted)      as converted_high_churn,
        count_if(not has_marketing_consent)                 as sent_without_consent
    from resp
    group by 1, 2
)
select
    d.event_date,
    d.campaign_id,
    c.campaign_name,
    c.channel,
    c.budget_php,
    d.sent, d.recipients_approx, d.opened, d.converted, d.median_hours_to_open,
    d.sent_high_churn, d.converted_high_churn, d.sent_without_consent,
    so.sent_by_segment,
    round(div0(d.opened, d.sent) * 100, 2)                  as open_rate_pct,
    round(div0(d.converted, d.sent) * 100, 3)               as conversion_rate_pct,
    round(div0(d.converted, d.opened) * 100, 3)             as open_to_conversion_pct,
    round(div0(c.budget_php, datediff('day', c.start_date, c.end_date) + 1), 2) as daily_budget_php,
    round(div0(div0(c.budget_php, datediff('day', c.start_date, c.end_date) + 1), d.converted), 2) as cost_per_conversion_php,
    d.event_date between c.start_date and c.end_date        as is_within_flight,
    current_timestamp()                                     as loaded_at
from daily d
left join {{ ref('slv_campaign') }} c on c.campaign_id = d.campaign_id
left join seg_obj so on so.event_date = d.event_date and so.campaign_id = d.campaign_id
