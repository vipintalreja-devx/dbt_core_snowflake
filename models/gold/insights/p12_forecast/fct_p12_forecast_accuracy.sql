{{ config(
    materialized='table',
    schema='gold',
    transient=false,
    copy_grants=true,
    tags=['insights', 'p12', 'forecast']
) }}
-- P12 Forecast accuracy: every past forecast compared with the actual once it is known.
-- Materialization: table (restated as actuals arrive; small).
-- Grain  : forecast_run_date x region_id x metric x forecast_date (only dates with actuals)
-- Sources: fct_p12_forecast (forecast history), int_p12_region_daily_series (actuals)
select
    f.forecast_run_date, f.metric, f.region_id, f.region_name, f.forecast_date, f.horizon_days,
    f.forecast_value, f.lower_80, f.upper_80,
    a.value                                                              as actual_value,
    round(a.value - f.forecast_value, 3)                                 as error,
    round(abs(a.value - f.forecast_value) / nullif(abs(a.value), 0) * 100, 2) as ape_pct,
    a.value between f.lower_80 and f.upper_80                            as within_interval
from {{ ref('fct_p12_forecast') }} f
join {{ ref('int_p12_region_daily_series') }} a
  on a.region_id = f.region_id and a.metric = f.metric and a.event_date = f.forecast_date
