{%- set ml_model  = this.database ~ '.' ~ this.schema ~ '.P12_FORECAST_MODEL' -%}
{{ config(
    materialized='incremental',
    schema='gold',
    incremental_strategy='delete+insert',
    unique_key='forecast_run_date',
    on_schema_change='append_new_columns',
    cluster_by=['forecast_run_date'],
    tags=['insights', 'p12', 'forecast'],
    pre_hook=[
      "create or replace view {{ this.database }}.{{ this.schema }}.P12_FORECAST_INPUT_V comment = 'P12 training data (last 180 complete days)' as
         select metric || '|' || region_id || '|' || region_name as series, event_date::timestamp_ntz as ts, value::float as y
           from {{ ref('int_p12_region_daily_series') }}
          where event_date > (select dateadd('day', -180, max(event_date)) from {{ ref('int_p12_region_daily_series') }})",
      "create or replace snowflake.ml.forecast {{ this.database }}.{{ this.schema }}.P12_FORECAST_MODEL(
         input_data => system$reference('VIEW', '{{ this.database }}.{{ this.schema }}.P12_FORECAST_INPUT_V'),
         series_colname => 'SERIES', timestamp_colname => 'TS', target_colname => 'Y',
         config_object => {'on_error': 'skip'})"
    ]
) }}
-- P12 30-day forecast of daily revenue and data traffic per region (Snowflake ML FORECAST,
-- gradient-boosted with automatic seasonality). The model is retrained on every run from the last
-- 180 complete days; each run's forecast is kept, so accuracy can be measured once actuals arrive.
-- Grain  : forecast_run_date x region_id x metric x forecast_date
-- Sources: int_p12_region_daily_series (via the training view P12_FORECAST_INPUT_V)
with fc as (
    select * from table({{ ml_model }}!forecast(forecasting_periods => 30, config_object => {'prediction_interval': 0.8}))
),
last_actual as (
    select max(event_date) as last_date from {{ ref('int_p12_region_daily_series') }}
)
select
    (select last_date from last_actual)                                  as forecast_run_date,
    split_part(fc.series::varchar, '|', 1)                               as metric,
    try_to_number(split_part(fc.series::varchar, '|', 2))                as region_id,
    split_part(fc.series::varchar, '|', 3)                               as region_name,
    fc.ts::date                                                          as forecast_date,
    datediff('day', (select last_date from last_actual), fc.ts::date)    as horizon_days,
    round(greatest(fc.forecast, 0), 3)                                   as forecast_value,
    round(greatest(fc.lower_bound, 0), 3)                                as lower_80,
    round(fc.upper_bound, 3)                                             as upper_80,
    'SNOWFLAKE.ML.FORECAST'                                              as method
from fc
