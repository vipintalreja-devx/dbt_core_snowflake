{{ config(
    materialized='incremental',
    schema='gold',
    tags=['insights', 'p04', 'roaming', 'mart'],
    incremental_strategy='delete+insert',
    unique_key='month_start',
    cluster_by=['month_start'],
    on_schema_change='append_new_columns'
) }}
-- P04 Roaming profitability: monthly scorecard per visited operator.
-- Purpose : which roaming partners and countries make or lose money: margin, margin rank within
--           the month, share of the month's roaming margin, month-over-month change, wholesale
--           cost mix by service and a LOSS_MAKING / THIN_MARGIN / HEALTHY flag.
-- Grain   : month_start x visited_country x partner_operator
-- Sources : fct_roaming_margin_daily
with monthly as (
    select date_trunc('month', event_date)::date                            as month_start,
           visited_country, partner_operator,
           sum(roaming_lines)                                               as roaming_line_days,
           sum(data_mb)                                                     as data_mb,
           sum(voice_min)                                                   as voice_min,
           sum(sms_count)                                                   as sms_count,
           sum(retail_revenue_php)                                          as retail_revenue_php,
           sum(wholesale_data_php)                                          as wholesale_data_php,
           sum(wholesale_voice_php)                                         as wholesale_voice_php,
           sum(wholesale_sms_php)                                           as wholesale_sms_php,
           sum(wholesale_cost_php)                                          as wholesale_cost_php,
           sum(margin_php)                                                  as margin_php,
           sum(fx_variance_php)                                             as fx_variance_php,
           sum(unpriced_records)                                            as unpriced_records
    from {{ ref('fct_roaming_margin_daily') }}
    -- one extra month of input so the month-over-month change is right for the first output month
    where event_date >= dateadd('month', -1, {{ f3_month_lower_bound('month_start') }})
    group by 1, 2, 3
)
select
    month_start, visited_country, partner_operator,
    roaming_line_days, data_mb, voice_min, sms_count,
    round(retail_revenue_php, 2)                                            as retail_revenue_php,
    round(wholesale_cost_php, 2)                                            as wholesale_cost_php,
    round(margin_php, 2)                                                    as margin_php,
    round(div0(margin_php, retail_revenue_php) * 100, 2)                    as margin_pct,
    round(div0(wholesale_data_php,  wholesale_cost_php) * 100, 1)           as cost_share_data_pct,
    round(div0(wholesale_voice_php, wholesale_cost_php) * 100, 1)           as cost_share_voice_pct,
    round(div0(wholesale_sms_php,   wholesale_cost_php) * 100, 1)           as cost_share_sms_pct,
    round(ratio_to_report(margin_php) over (partition by month_start) * 100, 2) as share_of_month_margin_pct,
    rank() over (partition by month_start order by margin_php desc)         as margin_rank,
    round(margin_php - lag(margin_php) over (partition by visited_country, partner_operator order by month_start), 2) as margin_mom_change_php,
    round(fx_variance_php, 2)                                               as fx_variance_php,
    unpriced_records,
    case when margin_php < 0 then 'LOSS_MAKING'
         when div0(margin_php, retail_revenue_php) < 0.15 then 'THIN_MARGIN'
         else 'HEALTHY' end                                                 as profitability_flag,
    {{ dbt_loaded_at() }}                                                   as _dbt_loaded_at
from monthly
qualify {{ f2_month_window('month_start', 'month_start') }}
