{{ config(
    materialized='table',
    schema='gold',
    transient=false,
    cluster_by=['region_id'],
    copy_grants=true,
    tags=['insights', 'p01', 'experience']
) }}
-- P01 current experience profile per line (last 30 days of the experience fact).
-- Materialization: table (current state).
-- Grain  : subscription_id
-- Sources: fct_p01_experience_daily, int_subscriber_profile (churn probability for correlation)
with last30 as (
    select *
    from {{ ref('fct_p01_experience_daily') }}
    where event_date > (select dateadd('day', -30, max(event_date)) from {{ ref('fct_p01_experience_daily') }})
)
select
    l.subscription_id,
    any_value(l.region_id)                                              as region_id,
    any_value(l.region_name)                                            as region_name,
    count(*)                                                            as active_days_30d,
    round(avg(l.experience_score), 2)                                   as avg_experience_30d,
    round(min(l.experience_score), 2)                                   as worst_experience_30d,
    min_by(l.event_date, l.experience_score)                            as worst_day,
    count_if(l.experience_band = 'POOR')                                as poor_days_30d,
    round(regr_slope(l.experience_score, datediff('day', '2000-01-01', l.event_date)), 4) as score_trend_per_day,
    max_by(l.experience_band, l.event_date)                             as latest_band,
    sum(l.outage_min)                                                   as outage_min_30d,
    sum(l.network_tickets)                                              as network_tickets_30d,
    any_value(p.churn_probability)                                      as churn_probability,
    case when count_if(l.experience_band = 'POOR') >= 5 and any_value(p.churn_probability) >= {{ var('churn_high_threshold') }} then 'SAVE_NOW'
         when count_if(l.experience_band = 'POOR') >= 5 then 'FIX_NETWORK'
         when regr_slope(l.experience_score, datediff('day', '2000-01-01', l.event_date)) < -0.5 then 'WATCH_DECLINE'
         else 'OK' end                                                  as experience_action
from last30 l
left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = l.subscription_id
group by l.subscription_id
