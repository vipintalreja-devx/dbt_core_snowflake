{{ config(
    materialized='incremental',
    schema='gold',
    incremental_strategy='delete+insert',
    unique_key='churn_month',
    on_schema_change='append_new_columns',
    tags=['insights', 'p02', 'churn']
) }}
-- P02 Churn by root cause, region and month, with the revenue that walked out of the door.
-- Grain  : churn_month x region_name x contract_type x root_cause
-- Sources: fct_p02_churn_attribution
select
    churn_month,
    region_id,
    region_name,
    contract_type,
    root_cause,
    count(*)                                                             as churned_lines,
    round(sum(avg_revenue_3m_php), 2)                                    as monthly_revenue_lost_php,
    round(count(*) / sum(count(*)) over (partition by churn_month, region_name, contract_type) * 100, 2) as share_of_churn_pct,
    count_if(matched_journey is not null)                                as lines_with_journey,
    round(avg(root_cause_confidence), 3)                                 as avg_confidence,
    round(avg(tickets), 2)                                               as avg_tickets,
    round(avg(poor_network_days), 2)                                     as avg_poor_network_days,
    round(median(tenure_months), 1)                                      as median_tenure_months
from {{ ref('fct_p02_churn_attribution') }}
where {{ f2_month_window('churn_month', 'churn_month') }}
group by 1, 2, 3, 4, 5
