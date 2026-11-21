{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='response_month',
    on_schema_change='append_new_columns',
    tags=['gold', 'analytics', 'care', 'nps', 'm22']
) }}
-- NPS and survey-driver scores per month, survey version and home region, split by whether the
-- survey followed a care ticket (transactional) or not (relationship NPS).
-- Ported from x_m22_nps_answers_monthly (PIPE_X.SP_X_M22_NPS_ANSWERS_MONTHLY).
-- Grain: response_month x survey_version x region_name x survey_type.
-- Sources: slv_nps_survey, fct_ticket, int_subscriber_profile.
with s as (
    select
        date_trunc('month', n.response_date)::date          as response_month,
        coalesce(n.survey_version, 'unknown')               as survey_version,
        coalesce(p.home_region_name, 'UNKNOWN')             as region_name,
        iff(n.ticket_id is null, 'RELATIONSHIP', 'TRANSACTIONAL') as survey_type,
        n.nps_score, n.nps_category, n.q_price, n.q_speed, n.q_support, n.q_coverage, n.would_recommend,
        t.category                                          as ticket_category,
        t.is_within_sla                                     as ticket_within_sla
    from {{ ref('slv_nps_survey') }} n
    left join {{ ref('fct_ticket') }} t             on t.ticket_id = n.ticket_id
    left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = n.subscription_id
    where {{ f2_month_window("date_trunc('month', n.response_date)::date", 'response_month') }}
)
select
    response_month,
    survey_version,
    region_name,
    survey_type,
    count(*)                                                as responses,
    count_if(nps_category = 'PROMOTER')                     as promoters,
    count_if(nps_category = 'PASSIVE')                      as passives,
    count_if(nps_category = 'DETRACTOR')                    as detractors,
    round(100 * div0(count_if(nps_category = 'PROMOTER') - count_if(nps_category = 'DETRACTOR'), count(*)), 2) as nps,
    round(avg(q_coverage), 2)                               as avg_coverage,
    round(avg(q_price), 2)                                  as avg_price,
    round(avg(q_speed), 2)                                  as avg_speed,
    round(avg(q_support), 2)                                as avg_support,
    round(div0(count_if(would_recommend), count(would_recommend)), 4) as recommend_rate,
    round(100 * div0(count_if(ticket_within_sla and nps_category = 'PROMOTER') - count_if(ticket_within_sla and nps_category = 'DETRACTOR'),
                     count_if(ticket_within_sla)), 2)       as nps_when_sla_met,
    round(100 * div0(count_if(not ticket_within_sla and nps_category = 'PROMOTER') - count_if(not ticket_within_sla and nps_category = 'DETRACTOR'),
                     count_if(not ticket_within_sla)), 2)   as nps_when_sla_missed,
    mode(iff(nps_category = 'DETRACTOR', ticket_category, null)) as top_detractor_ticket_category,
    current_timestamp()                                     as loaded_at
from s
group by 1, 2, 3, 4
