{{ config(
    materialized='table',
    cluster_by=['as_of_date'],
    tags=['gold', 'aggregates', 'ref', 'product', 'e25']
) }}
-- Tariff plans on sale per day x contract type x internet service, with the current base on them.
-- Ported from x_e25_plan_active_daily (PIPE_X.SP_X_E25_PLAN_ACTIVE_DAILY); a date spine replaces the
-- single run_date so every day in the window is present.
-- Grain   : as_of_date x contract_type x internet_service
-- Sources : dim_date (spine), slv_plan, int_subscriber_profile
with days as (
    select date_day as as_of_date
    from {{ ref('dim_date') }}
    where date_day between '{{ var("history_start_date") }}'::date
                       and (select max(business_date) from {{ ref('fct_revenue_daily') }})
),
base as (
    select plan_id, count_if(status in ('ACTIVE', 'SUSPENDED')) as active_lines
    from {{ ref('int_subscriber_profile') }}
    group by 1
),
on_sale as (
    select d.as_of_date, p.plan_id, p.contract_type, coalesce(p.internet_service, 'NONE') as internet_service,
           p.base_price_php, p.is_promo, p.speed_mbps, p.data_cap_gb, p.feature_count,
           zeroifnull(b.active_lines) as active_lines,
           p.effective_from = d.as_of_date as launched_today
    from days d
    join {{ ref('slv_plan') }} p
      on d.as_of_date between p.effective_from and coalesce(p.effective_to, '9999-12-31'::date)
    left join base b on b.plan_id = p.plan_id
)
select
    as_of_date, contract_type, internet_service,
    count(*)                                                as plans,
    count_if(is_promo)                                      as promo_plans,
    count_if(launched_today)                                as plans_launched,
    round(avg(base_price_php), 2)                           as avg_base_price_php,
    median(base_price_php)                                  as median_base_price_php,
    min(base_price_php)                                     as min_base_price_php,
    max(base_price_php)                                     as max_base_price_php,
    round(avg(speed_mbps), 0)                               as avg_speed_mbps,
    round(avg(feature_count), 1)                            as avg_feature_count,
    sum(active_lines)                                       as active_lines_on_plans,
    max_by(plan_id, active_lines)                           as most_popular_plan_id,
    round(div0(sum(base_price_php * active_lines), sum(active_lines)), 2) as base_weighted_price_php,
    count(*) - lag(count(*)) over (partition by contract_type, internet_service order by as_of_date) as plans_dod_change,
    current_timestamp()                                     as loaded_at
from on_sale
group by 1, 2, 3
