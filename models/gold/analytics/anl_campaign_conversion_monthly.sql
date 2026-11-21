{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='send_month',
    on_schema_change='append_new_columns',
    cluster_by=['send_month'],
    tags=['gold', 'analytics', 'marketing', 'campaigns', 'm09']
) }}
-- Campaign effectiveness per send month: reach, open / conversion funnel, cost per conversion and
-- revenue uplift of converters (30 days after vs. 30 days before the send).
-- Ported from x_m09_campaign_conversion_monthly (PIPE_X.SP_X_M09_CAMPAIGN_CONVERSION_MONTHLY).
-- Grain: send_month x campaign_id.
-- Sources: slv_campaign_response, slv_campaign, fct_revenue_daily, int_subscriber_profile.
with resp as (
    select
        r.response_id, r.campaign_id, r.subscription_id, r.event_date as sent_date,
        date_trunc('month', r.event_date)::date             as send_month,
        r.is_opened, r.is_converted, r.hours_to_open
    from {{ ref('slv_campaign_response') }} r
    where r.event_date >= '{{ var("history_start_date") }}'::date
      -- converters' 30-day post-send revenue keeps landing for a month after the send, so one
      -- extra month is restated on every incremental run
      and date_trunc('month', r.event_date)::date >= dateadd('month', -1, {{ f3_month_lower_bound('send_month') }})
),
conv as (
    select response_id, campaign_id, send_month, subscription_id, sent_date
    from resp
    where is_converted
),
uplift as (
    select
        c.campaign_id,
        c.send_month,
        sum(iff(f.business_date >  c.sent_date, f.total_revenue_php, 0)) as revenue_30d_after_php,
        sum(iff(f.business_date <= c.sent_date, f.total_revenue_php, 0)) as revenue_30d_before_php
    from conv c
    join {{ ref('fct_revenue_daily') }} f
      on f.subscription_id = c.subscription_id
     and f.business_date between dateadd('day', -29, c.sent_date) and dateadd('day', 30, c.sent_date)
    group by 1, 2
),
funnel as (
    select
        r.send_month,
        r.campaign_id,
        count(*)                                            as sends,
        count(distinct r.subscription_id)                   as reached_lines,
        count_if(r.is_opened)                               as opened,
        count_if(r.is_converted)                            as converted,
        round(median(r.hours_to_open), 2)                   as median_hours_to_open,
        count(distinct p.home_region_name)                  as regions_reached,
        round(avg(p.churn_probability), 4)                  as avg_churn_prob_targeted,
        count_if(p.churn_band = 'HIGH')                     as high_risk_reached
    from resp r
    left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = r.subscription_id
    group by 1, 2
)
select
    f.send_month,
    f.campaign_id,
    c.campaign_name,
    c.channel,
    c.start_date,
    c.end_date,
    c.budget_php,
    f.sends,
    f.reached_lines,
    f.opened,
    f.converted,
    round(div0(f.opened, f.sends), 4)                       as open_rate,
    round(div0(f.converted, f.sends), 4)                    as conversion_rate,
    round(div0(f.converted, f.opened), 4)                   as open_to_convert_rate,
    f.median_hours_to_open,
    f.regions_reached,
    f.avg_churn_prob_targeted,
    f.high_risk_reached,
    round(zeroifnull(u.revenue_30d_after_php), 2)           as converter_revenue_30d_after_php,
    round(zeroifnull(u.revenue_30d_before_php), 2)          as converter_revenue_30d_before_php,
    round(zeroifnull(u.revenue_30d_after_php) - zeroifnull(u.revenue_30d_before_php), 2) as converter_revenue_uplift_php,
    round(div0(c.budget_php, f.converted), 2)               as cost_per_conversion_php,
    round(div0(zeroifnull(u.revenue_30d_after_php) - zeroifnull(u.revenue_30d_before_php), c.budget_php), 4) as uplift_roi,
    rank() over (partition by f.send_month, c.channel order by div0(f.converted, f.sends) desc) as conversion_rank_in_channel,
    current_timestamp()                                     as loaded_at
from funnel f
join {{ ref('slv_campaign') }} c on c.campaign_id = f.campaign_id
left join uplift u on u.campaign_id = f.campaign_id and u.send_month = f.send_month
