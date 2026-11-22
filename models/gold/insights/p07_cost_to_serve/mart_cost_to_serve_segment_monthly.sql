{{ config(
    materialized='incremental',
    schema='gold',
    tags=['insights', 'p07', 'care', 'cost', 'mart'],
    incremental_strategy='delete+insert',
    unique_key='month_start',
    cluster_by=['month_start'],
    on_schema_change='append_new_columns'
) }}
-- P07 Cost-to-serve: segment view per month.
-- Purpose : revenue, care cost by type, cost per line, 90th-percentile cost per line and the
--           share of unprofitable lines for every region x plan category x tenure band x churn band.
-- Grain   : month_start x region_id x plan_category x tenure_band x churn_band
-- Sources : fct_line_cost_to_serve_monthly
select
    month_start, region_id,
    coalesce(plan_category, 'UNKNOWN')                                      as plan_category,
    tenure_band,
    coalesce(churn_band, 'UNSCORED')                                        as churn_band,
    count(*)                                                                as lines,
    count_if(care_cost_php > 0)                                             as lines_with_care,
    round(sum(revenue_php), 2)                                              as revenue_php,
    round(sum(ticket_cost_php), 2)                                          as ticket_cost_php,
    round(sum(reopen_cost_php), 2)                                          as reopen_cost_php,
    round(sum(note_cost_php), 2)                                            as note_cost_php,
    round(sum(field_cost_php), 2)                                           as field_cost_php,
    round(sum(care_cost_php), 2)                                            as care_cost_php,
    round(div0(sum(care_cost_php), count(*)), 2)                            as care_cost_per_line_php,
    round(approx_percentile(care_cost_php, 0.9), 2)                         as p90_care_cost_per_line_php,
    round(div0(sum(care_cost_php), sum(revenue_php)) * 100, 2)              as care_cost_pct_of_revenue,
    round(div0(count_if(is_unprofitable), count(*)) * 100, 2)               as unprofitable_lines_pct,
    {{ dbt_loaded_at() }}                                                   as _dbt_loaded_at
from {{ ref('fct_line_cost_to_serve_monthly') }}
where {{ f2_month_window('month_start', 'month_start') }}
group by 1, 2, 3, 4, 5
