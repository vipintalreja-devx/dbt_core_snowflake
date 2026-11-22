{{ config(
    materialized='incremental',
    schema='gold',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['insights', 'p01', 'experience']
) }}
-- P01 regional experience KPIs per day (full loaded history of the rolling fact is aggregated).
-- Grain  : event_date x region_id
-- Sources: fct_p01_experience_daily
select
    event_date,
    region_id,
    any_value(region_name)                                              as region_name,
    count(*)                                                            as active_lines,
    round(avg(experience_score), 2)                                     as avg_experience_score,
    round(approx_percentile(experience_score, 0.1), 2)                  as p10_experience_score,
    round(count_if(experience_band = 'POOR') / count(*) * 100, 2)       as poor_line_pct,
    round(count_if(experience_band = 'GOOD') / count(*) * 100, 2)       as good_line_pct,
    sum(outage_min)                                                     as outage_line_minutes,
    sum(network_tickets)                                                as network_tickets,
    round(avg(degraded_usage_share) * 100, 2)                           as degraded_usage_pct,
    round(avg(avg(experience_score)) over (partition by region_id order by event_date
                                           rows between 6 preceding and current row), 2) as avg_score_7d
from {{ ref('fct_p01_experience_daily') }}
where event_date >= {{ f1_lower_bound('event_date', 7) }}
group by 1, 2
qualify event_date >= {{ f1_lower_bound('event_date') }}
