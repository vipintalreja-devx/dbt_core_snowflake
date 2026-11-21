{{ config(
    materialized='incremental',
    schema='silver',
    tags=['silver', 'care', 'cdc'],
    transient=true,
    unique_key='survey_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('response_date') }}",
               "{{ cdc_log_deletes('survey_id', 'response_date') }}",
               "{{ cdc_purge_tombstones() }}"]
) }}
-- NPS survey responses. CDC from bronze (DBT_ROLE is exempt from the aggregation policy).
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
    answers:survey_version::string                          as survey_version,
    _cdc_action,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ cdc_source('care', 'nps_survey') }}
where score between 0 and 10
qualify row_number() over (partition by survey_id order by iff(_cdc_action = 'DELETE', 1, 0) desc) = 1
