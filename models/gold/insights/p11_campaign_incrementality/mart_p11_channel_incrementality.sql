{{ config(
    materialized='table',
    schema='gold',
    tags=['insights', 'p11'],
    transient=false,
    copy_grants=true
) }}
-- P11 campaign incrementality | output 2: which channels create incremental revenue.
-- Grain   : channel x send_month (only months whose 30-day post window is complete)
-- Sources : fct_p11_campaign_uplift
select
    channel,
    send_month,
    count(*)                                                as campaigns,
    sum(treated_lines)                                      as treated_lines,
    round(avg(conversion_rate), 4)                          as avg_conversion_rate,
    round(median(uplift_per_line_php), 2)                   as median_uplift_per_line_php,
    round(percentile_cont(0.25) within group (order by uplift_per_line_php), 2) as p25_uplift_per_line_php,
    round(percentile_cont(0.75) within group (order by uplift_per_line_php), 2) as p75_uplift_per_line_php,
    round(sum(incremental_revenue_php), 2)                  as incremental_revenue_php,
    round(sum(budget_allocated_php), 2)                     as budget_allocated_php,
    round(sum(incremental_revenue_php) / nullif(sum(budget_allocated_php), 0), 3) as incremental_roi,
    round(avg(iff(is_significant and uplift_per_line_php > 0, 1, 0)), 4) as share_significant_positive,
    round(avg(iff(is_significant and uplift_per_line_php < 0, 1, 0)), 4) as share_significant_negative,
    max_by(campaign_name, incremental_revenue_php)          as best_campaign,
    current_timestamp()                                     as loaded_at
from {{ ref('fct_p11_campaign_uplift') }}
where is_post_window_complete
group by 1, 2
