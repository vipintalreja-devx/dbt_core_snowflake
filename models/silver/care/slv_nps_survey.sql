{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'care'],
    transient=true
) }}
select
    survey_id,
    subscription_id,
    ticket_id,
    {{ tz_to_utc('responded_at') }}                         as responded_at_utc,
    {{ utc_to_local(tz_to_utc('responded_at')) }}::date     as response_date,
    score                                                   as nps_score,
    case when score >= 9 then 'PROMOTER' when score >= 7 then 'PASSIVE' when score >= 0 then 'DETRACTOR' end as nps_category,
    verbatim,
    answers:q_price::number                                 as q_price,
    answers:q_speed::number                                 as q_speed,
    answers:q_support::number                               as q_support,
    answers:q_coverage::number                              as q_coverage,
    answers:would_recommend::boolean                        as would_recommend,
    answers:survey_version::string                          as survey_version
from {{ source('care', 'nps_survey') }}
where score between 0 and 10
