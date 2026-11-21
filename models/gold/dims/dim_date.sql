{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dims', 'dimension'],
    transient=false,
    copy_grants=true,
    post_hook="alter table {{ this }} set comment = 'Gold dimension - dim_date (rebuilt {{ run_started_at.strftime(\'%Y-%m-%d %H:%M\') }} UTC)'"
) }}
-- Materialization: table (current-state/small reference): dimension (small, current state; history comes from the SCD2 snapshots).
-- Calendar dimension (Manila business calendar).
with spine as (
    {{ dbt_utils.date_spine(datepart="day",
                            start_date="'2015-01-01'::date",
                            end_date="'2028-01-01'::date") }}
)
select
    date_day::date                                          as date_day,
    to_number(to_char(date_day, 'YYYYMMDD'))                as date_key,
    year(date_day)                                          as year_num,
    quarter(date_day)                                       as quarter_num,
    month(date_day)                                         as month_num,
    to_char(date_day, 'Mon YYYY')                           as month_label,
    date_trunc('month', date_day)::date                     as month_start,
    last_day(date_day)::date                                as month_end,
    date_trunc('week', date_day)::date                      as week_start,
    dayofweekiso(date_day)                                  as iso_day_of_week,
    dayname(date_day)                                       as day_name,
    dayofweekiso(date_day) in (6, 7)                        as is_weekend,
    day(date_day)                                           as day_of_month,
    datediff('day', date_trunc('month', date_day), last_day(date_day)) + 1 as days_in_month
from spine
