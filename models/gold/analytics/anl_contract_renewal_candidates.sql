{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='run_month',
    on_schema_change='append_new_columns',
    cluster_by=['run_month'],
    tags=['gold', 'analytics', 'retention', 'contracts', 'm36']
) }}
-- Retention work-list: for each month, contracts that expire NEXT month, scored by churn risk,
-- value, recent care pain and payment behaviour into a priority and a recommended action.
-- Ported from x_m36_contract_renewal_candidates (PIPE_X.SP_X_M36_CONTRACT_RENEWAL_CANDIDATES).
-- Grain: run_month x contract_id.
-- Sources: slv_contract, int_subscriber_profile, slv_churn_score, fct_subscriber_monthly, fct_ticket,
--          fct_invoice, slv_plan.
with months as (
    select distinct month_start as run_month
    from {{ ref('fct_subscriber_monthly') }}
    where {{ f2_month_window('month_start', 'run_month') }}
),
expiring as (
    select m.run_month, c.*
    from months m
    join {{ ref('slv_contract') }} c
      on c.end_date >= dateadd('month', 1, m.run_month)
     and c.end_date <  dateadd('month', 2, m.run_month)
     and (c.terminated_on is null or c.terminated_on > last_day(m.run_month))
),
value3m as (
    select e.run_month, e.subscription_id,
           sum(f.total_revenue_php)                         as revenue_3m_php,
           avg(f.data_mb) / 1024                            as avg_data_gb_3m,
           regr_slope(f.total_revenue_php, datediff('month', '2000-01-01'::date, f.month_start)) as revenue_trend_slope
    from expiring e
    join {{ ref('fct_subscriber_monthly') }} f
      on f.subscription_id = e.subscription_id
     and f.month_start between dateadd('month', -2, e.run_month) and e.run_month
    group by 1, 2
),
care as (
    select e.run_month, e.subscription_id,
           count(*)                                         as tickets_90d,
           count_if(not t.is_within_sla)                    as sla_misses_90d,
           count_if(t.is_open)                              as open_tickets
    from expiring e
    join {{ ref('fct_ticket') }} t
      on t.subscription_id = e.subscription_id
     and t.opened_date between dateadd('day', -89, last_day(e.run_month)) and last_day(e.run_month)
    group by 1, 2
),
pay as (
    select e.run_month, e.subscription_id,
           count_if(not i.is_paid_on_time)                  as late_invoices_6m,
           sum(i.outstanding_php)                           as outstanding_php
    from expiring e
    join {{ ref('int_subscriber_profile') }} p on p.subscription_id = e.subscription_id
    join {{ ref('fct_invoice') }} i
      on i.account_id = p.account_id
     and i.billing_month between dateadd('month', -5, e.run_month) and e.run_month
    group by 1, 2
),
scored as (
    select
        e.run_month,
        e.contract_id,
        e.subscription_id,
        p.account_id,
        e.contract_type,
        e.end_date,
        e.is_auto_renew,
        p.plan_id,
        pl.plan_name,
        pl.base_price_php,
        p.home_region_name,
        p.segment_code,
        p.tenure_months,
        s.churn_probability,
        s.churn_band,
        zeroifnull(v.revenue_3m_php)                        as revenue_3m_php,
        round(v.avg_data_gb_3m, 3)                          as avg_data_gb_3m,
        round(v.revenue_trend_slope, 2)                     as revenue_trend_slope,
        zeroifnull(c.tickets_90d)                           as tickets_90d,
        zeroifnull(c.sla_misses_90d)                        as sla_misses_90d,
        zeroifnull(c.open_tickets)                          as open_tickets,
        zeroifnull(py.late_invoices_6m)                     as late_invoices_6m,
        round(zeroifnull(py.outstanding_php), 2)            as outstanding_php
    from expiring e
    join {{ ref('int_subscriber_profile') }} p on p.subscription_id = e.subscription_id
                                              and p.is_reportable and p.status = 'ACTIVE'
    left join {{ ref('slv_plan') }} pl         on pl.plan_id = p.plan_id
    left join {{ ref('slv_churn_score') }} s   on s.subscription_id = e.subscription_id
    left join value3m v on v.run_month = e.run_month and v.subscription_id = e.subscription_id
    left join care c    on c.run_month = e.run_month and c.subscription_id = e.subscription_id
    left join pay py    on py.run_month = e.run_month and py.subscription_id = e.subscription_id
)
select
    sc.*,
    round(
        coalesce(sc.churn_probability, 0) * 50
      + least(sc.revenue_3m_php / nullif(percentile_cont(0.9) within group (order by sc.revenue_3m_php)
                                         over (partition by sc.run_month), 0), 1) * 30
      + least(sc.sla_misses_90d, 3) * 5
      + iff(sc.is_auto_renew, 0, 5)
    , 2)                                                    as priority_score,
    case
        when sc.outstanding_php > 0 and sc.late_invoices_6m >= 3     then 'COLLECT_BEFORE_RENEW'
        when sc.open_tickets > 0 or sc.sla_misses_90d > 0           then 'FIX_SERVICE_THEN_RENEW'
        when coalesce(sc.churn_probability, 0) >= {{ var('churn_high_threshold') }} then 'RETENTION_OFFER'
        when not sc.is_auto_renew                                    then 'RENEWAL_CALL'
        else 'AUTO_RENEW_NOTICE'
    end                                                     as recommended_action,
    current_timestamp()                                     as loaded_at
from scored sc
qualify row_number() over (partition by sc.run_month, sc.contract_id order by sc.end_date) = 1
