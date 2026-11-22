{{ config(
    materialized='table',
    schema='gold',
    transient=false,
    copy_grants=true,
    tags=['insights', 'p12', 'forecast']
) }}
-- P12 Regional outlook: next 7 / 30 days forecast vs the last 30 actual days, with the latest
-- measured accuracy of the model for that region and metric.
-- Materialization: table (current outlook).
-- Grain  : region_id x metric
-- Sources: fct_p12_forecast (latest run), int_p12_region_daily_series, fct_p12_forecast_accuracy
with latest as (
    select * from {{ ref('fct_p12_forecast') }}
    where forecast_run_date = (select max(forecast_run_date) from {{ ref('fct_p12_forecast') }})
),
fc as (
    select region_id, region_name, metric, forecast_run_date,
           sum(iff(horizon_days <= 7, forecast_value, 0))                as next_7d,
           sum(forecast_value)                                           as next_30d,
           sum(lower_80)                                                 as next_30d_low,
           sum(upper_80)                                                 as next_30d_high
    from latest group by 1, 2, 3, 4
),
act as (
    select region_id, metric, sum(value) as last_30d
    from {{ ref('int_p12_region_daily_series') }}
    where event_date > (select dateadd('day', -30, max(event_date)) from {{ ref('int_p12_region_daily_series') }})
    group by 1, 2
),
acc as (
    select region_id, metric, round(avg(ape_pct), 2) as mape_pct, round(avg(iff(within_interval, 1, 0)) * 100, 1) as interval_coverage_pct
    from {{ ref('fct_p12_forecast_accuracy') }}
    group by 1, 2
)
select
    fc.region_id, fc.region_name, fc.metric, fc.forecast_run_date,
    round(a.last_30d, 2)                                                 as actual_last_30d,
    round(fc.next_7d, 2)                                                 as forecast_next_7d,
    round(fc.next_30d, 2)                                                as forecast_next_30d,
    round(fc.next_30d_low, 2)                                            as forecast_next_30d_low,
    round(fc.next_30d_high, 2)                                           as forecast_next_30d_high,
    round((fc.next_30d / nullif(a.last_30d, 0) - 1) * 100, 2)            as expected_change_pct,
    acc.mape_pct,
    acc.interval_coverage_pct
from fc
left join act a   on a.region_id = fc.region_id and a.metric = fc.metric
left join acc acc on acc.region_id = fc.region_id and acc.metric = fc.metric
