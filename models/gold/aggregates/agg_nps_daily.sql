{{ config(
    materialized='table',
    cluster_by=['response_date'],
    tags=['gold', 'aggregates', 'care', 'customer_experience', 'e08']
) }}
-- Net Promoter Score per day x region x brand, with driver scores and ticket linkage.
-- Ported from x_e08_nps_daily (PIPE_X.SP_X_E08_NPS_DAILY).
-- NPS = %promoters (9-10) - %detractors (0-6). 7-day NPS is computed from 7-day counts (not an average of daily NPS).
-- Grain   : response_date x region_name x brand_name
-- Sources : slv_nps_survey, int_subscriber_profile, slv_ticket
with s as (
    select
        n.survey_id, n.subscription_id, n.response_date, n.nps_score, n.nps_category,
        n.q_price, n.q_speed, n.q_support, n.q_coverage, n.would_recommend,
        tk.category                                         as ticket_category,
        coalesce(p.home_region_name, 'UNKNOWN')             as region_name,
        coalesce(p.brand_name, 'OWN BRAND')                 as brand_name
    from {{ ref('slv_nps_survey') }} n
    left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = n.subscription_id
    left join {{ ref('slv_ticket') }} tk             on tk.ticket_id = n.ticket_id
    where n.response_date >= dateadd('day', -7, '{{ var("history_start_date") }}'::date)
),
daily as (
    select
        response_date, region_name, brand_name,
        count(*)                                            as responses,
        round(avg(nps_score), 2)                            as avg_score,
        count_if(nps_category = 'PROMOTER')                 as promoters,
        count_if(nps_category = 'PASSIVE')                  as passives,
        count_if(nps_category = 'DETRACTOR')                as detractors,
        round(avg(q_price), 2)                              as avg_q_price,
        round(avg(q_speed), 2)                              as avg_q_speed,
        round(avg(q_support), 2)                            as avg_q_support,
        round(avg(q_coverage), 2)                           as avg_q_coverage,
        count_if(ticket_category is not null)               as responses_after_ticket,
        mode(ticket_category)                               as most_common_ticket_category
    from s
    group by 1, 2, 3
),
rolled as (
    select
        d.*,
        round(100 * div0(d.promoters - d.detractors, d.responses), 1) as nps,
        sum(d.responses)  over (partition by d.region_name, d.brand_name order by d.response_date rows between 6 preceding and current row)                           as responses_7d,
        sum(d.promoters)  over (partition by d.region_name, d.brand_name order by d.response_date rows between 6 preceding and current row)                           as promoters_7d,
        sum(d.detractors) over (partition by d.region_name, d.brand_name order by d.response_date rows between 6 preceding and current row)                           as detractors_7d
    from daily d
)
select
    r.*,
    round(100 * div0(r.promoters_7d - r.detractors_7d, r.responses_7d), 1) as nps_7d,
    -- weakest driver of the day (lowest average answer)
    array_construct('PRICE', 'SPEED', 'SUPPORT', 'COVERAGE')[
        array_position(least(r.avg_q_price, r.avg_q_speed, r.avg_q_support, r.avg_q_coverage)::variant,
                       array_construct(r.avg_q_price, r.avg_q_speed, r.avg_q_support, r.avg_q_coverage))]::string as weakest_driver,
    current_timestamp()                                     as loaded_at
from rolled r
where r.response_date >= '{{ var("history_start_date") }}'::date
