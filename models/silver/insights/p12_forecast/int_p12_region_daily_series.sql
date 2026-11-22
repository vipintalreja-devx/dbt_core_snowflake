{{ config(
    materialized='incremental',
    schema='silver',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['insights', 'p12', 'forecast'],
    post_hook=["delete from {{ this }} where event_date < (select dateadd('day', -400, max(event_date)) from {{ this }})"]
) }}
-- P12 Usage & revenue forecasting, step 1: clean daily training series per region and metric.
-- Grain  : event_date x region_id x metric (REVENUE_PHP, DATA_GB)
-- Sources: agg_usage_daily_totals (daily totals per region; no scan of the 485M-row usage fact),
--          dim_region (region id)
-- The latest day is excluded while it is still incomplete (fewer than 80% of the trailing 7-day average lines).
-- Window : full refresh var('insights_forecast_train_days', 365) days; then incremental, 400-day retention.
with base as (
    select a.event_date, coalesce(r.region_id, -1) as region_id, a.region_name,
           a.revenue_php, a.data_gb, a.active_lines
    from {{ ref('agg_usage_daily_totals') }} a
    left join {{ ref('dim_region') }} r on upper(r.region_name) = upper(a.region_name)
    where a.event_date >= {% if is_incremental() %}{{ f1_lower_bound('event_date', 8) }}{% else %}(select dateadd('day', -{{ var('insights_forecast_train_days', 365) | int + 8 }}, max(event_date)) from {{ ref('agg_usage_daily_totals') }}){% endif %}
),
days as (
    select event_date, sum(active_lines) as day_lines,
           avg(sum(active_lines)) over (order by event_date rows between 7 preceding and 1 preceding) as med_lines_7d
    from base group by 1
),
complete as (
    select b.* from base b join days d on d.event_date = b.event_date
    where d.med_lines_7d is null or d.day_lines >= 0.8 * d.med_lines_7d
)
select event_date, region_id, region_name, 'REVENUE_PHP' as metric, round(revenue_php, 2) as value from complete
union all
select event_date, region_id, region_name, 'DATA_GB', round(data_gb, 3) from complete
