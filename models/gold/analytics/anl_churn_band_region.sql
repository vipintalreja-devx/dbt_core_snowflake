{{ config(
    materialized='table',
    tags=['gold', 'analytics', 'churn', 'm10']
) }}
-- Materialization: table (current-state/small reference): current-state profile of the subscriber/device base.
-- Churn-risk exposure per home region and risk band from the latest model score: lines, probability
-- distribution, revenue at risk (last closed month revenue x probability) and open-ticket overlap.
-- Ported from x_m10_churn_band_region (PIPE_X.SP_X_M10_CHURN_BAND_REGION).
-- Grain: region_name x churn_band x contract_type.
-- Sources: int_subscriber_profile, slv_churn_score, fct_subscriber_monthly, fct_ticket.
with last_month as (
    select max(month_start) as month_start from {{ ref('fct_subscriber_monthly') }}
),
rev as (
    select m.subscription_id, m.total_revenue_php
    from {{ ref('fct_subscriber_monthly') }} m
    where m.month_start = (select month_start from last_month)
),
open_tk as (
    select subscription_id, count(*) as open_tickets
    from {{ ref('fct_ticket') }}
    where is_open
    group by 1
),
scored as (
    select
        p.subscription_id,
        p.home_region_name,
        p.island_group,
        coalesce(p.contract_type, 'UNKNOWN')                as contract_type,
        s.churn_probability,
        s.churn_band,
        s.score_date,
        p.monthly_charges_php,
        zeroifnull(r.total_revenue_php)                     as revenue_last_month_php,
        zeroifnull(o.open_tickets)                          as open_tickets
    from {{ ref('int_subscriber_profile') }} p
    join {{ ref('slv_churn_score') }} s on s.subscription_id = p.subscription_id
    left join rev r     on r.subscription_id = p.subscription_id
    left join open_tk o on o.subscription_id = p.subscription_id
    where p.is_reportable
      and p.status in ('ACTIVE', 'SUSPENDED')
)
select
    home_region_name                                        as region_name,
    any_value(island_group)                                 as island_group,
    churn_band,
    contract_type,
    max(score_date)                                         as score_date,
    count(*)                                                as lines,
    round(avg(churn_probability), 4)                        as avg_probability,
    round(approx_percentile(churn_probability, 0.5), 4)     as p50_probability,
    round(approx_percentile(churn_probability, 0.9), 4)     as p90_probability,
    round(avg(monthly_charges_php), 2)                      as avg_monthly_charges_php,
    round(sum(revenue_last_month_php), 2)                   as revenue_last_month_php,
    round(sum(revenue_last_month_php * churn_probability), 2) as expected_revenue_at_risk_php,
    count_if(open_tickets > 0)                              as lines_with_open_ticket,
    round(div0(count_if(open_tickets > 0), count(*)), 4)    as open_ticket_share,
    round(ratio_to_report(count(*)) over (partition by home_region_name), 4) as band_share_in_region,
    round(ratio_to_report(sum(revenue_last_month_php * churn_probability)) over (), 5) as share_of_national_risk,
    current_timestamp()                                     as loaded_at
from scored
group by home_region_name, churn_band, contract_type
