{{ config(
    materialized='table',
    schema='gold',
    cluster_by=['value_tier'],
    tags=['gold', 'dims', 'customer', 'g3'],
    transient=false
) }}
-- Materialization: table (current-state/small reference): dimension (small, current state; history comes from the SCD2 snapshots).
-- Customer value tier (PLATINUM / GOLD / SILVER / BRONZE) from revenue, usage, engagement,
-- payment behaviour and care load over the loaded window.
-- Ported from telco_v2_core g3_dim_customer_tier (PIPE.SP_GOLD_CUSTOMER_TIER); repointed from
-- raw DAILY_REVENUE / USAGE_DAILY_AGG to gold fct_subscriber_monthly and silver.
-- Grain: customer (reportable customers with revenue in the latest month).
-- Sources (8): fct_subscriber_monthly, int_subscriber_profile, slv_payment, slv_invoice,
-- slv_account, slv_segment_membership, fct_ticket, dim_customer.
with sub_cust as (
    select subscription_id, account_id, customer_id
    from {{ ref('int_subscriber_profile') }}
    where is_reportable
),
last_month as (
    select max(month_start) as m from {{ ref('fct_subscriber_monthly') }}
),
monthly as (
    select sc.customer_id, m.month_start,
           sum(m.total_revenue_php)                             as rev,
           sum(m.data_mb)                                       as data_mb,
           sum(m.voice_min)                                     as voice_min,
           sum(m.active_days)                                   as active_days
    from {{ ref('fct_subscriber_monthly') }} m
    join sub_cust sc on sc.subscription_id = m.subscription_id
    group by 1, 2
),
rev_trend as (
    select customer_id,
           sum(rev)                                             as revenue_window_php,
           sum(data_mb)                                         as data_mb_window,
           sum(voice_min)                                       as voice_min_window,
           sum(active_days)                                     as active_days_window,
           count(*)                                             as months_with_revenue,
           max(decl)                                            as declining_months,
           max(iff(month_start = (select m from last_month), 1, 0)) as active_last_month,
           regr_slope(rev, datediff('month', '2000-01-01'::date, month_start)) as revenue_slope_php_per_month
    from (select ml.*,
                 conditional_true_event(ml.rev < 0.8 * ml.prev_rev)
                     over (partition by ml.customer_id order by ml.month_start) as decl
          from (select mo.*, lag(mo.rev) over (partition by mo.customer_id order by mo.month_start) as prev_rev
                from monthly mo) ml)
    group by 1
),
tickets as (
    select sc.customer_id, count(*) as tickets_window, count_if(t.is_open) as open_tickets
    from {{ ref('fct_ticket') }} t
    join sub_cust sc on sc.subscription_id = t.subscription_id
    where t.opened_date >= '{{ var("history_start_date") }}'::date
    group by 1
),
payments as (
    select a.customer_id, count(*) as payments,
           avg(datediff('day', i.issue_date, p.payment_date))   as avg_days_to_pay,
           count_if(p.payment_date > i.due_date)                as late_payments
    from {{ ref('slv_payment') }} p
    join {{ ref('slv_invoice') }} i on i.invoice_id = p.invoice_id
    join {{ ref('slv_account') }} a on a.account_id = p.account_id
    where p.payment_date >= dateadd('month', -12, (select m from last_month))
    group by 1
),
segment as (
    select customer_id, segment_code from (
        select sc.customer_id, sm.segment_code, count(*) as n
        from {{ ref('slv_segment_membership') }} sm
        join sub_cust sc on sc.subscription_id = sm.subscription_id
        where sm.is_current
        group by 1, 2)
    qualify row_number() over (partition by customer_id order by n desc, segment_code) = 1
),
cust as (
    select customer_id, min(account_id) as account_id, count(*) as lines from sub_cust group by 1
),
base as (
    select
        c.customer_id, c.account_id, c.lines,
        coalesce(sg.segment_code, 'NONE')                       as segment_code,
        case when zeroifnull(py.payments) = 0 then 'NO_PAYMENTS'
             when py.avg_days_to_pay <= 3    then 'FAST'
             when py.avg_days_to_pay >= 10   then 'NEAR_DUE'
             else 'NORMAL' end                                  as payment_behaviour,
        rt.revenue_window_php, rt.months_with_revenue, rt.declining_months,
        round(rt.revenue_slope_php_per_month, 2)                as revenue_slope_php_per_month,
        zeroifnull(rt.data_mb_window)                           as data_mb_window,
        zeroifnull(rt.voice_min_window)                         as voice_min_window,
        zeroifnull(t.tickets_window)                            as tickets_window,
        zeroifnull(t.open_tickets)                              as open_tickets,
        round(py.avg_days_to_pay, 2)                            as avg_days_to_pay,
        zeroifnull(py.late_payments)                            as late_payments,
        dc.age_band, dc.gender, dc.has_marketing_consent
    from cust c
    join rev_trend rt on rt.customer_id = c.customer_id and rt.active_last_month = 1
    left join tickets t     on t.customer_id = c.customer_id
    left join payments py   on py.customer_id = c.customer_id
    left join segment sg    on sg.customer_id = c.customer_id
    left join {{ ref('dim_customer') }} dc on dc.customer_id = c.customer_id
),
scored as (
    select b.*,
           round(0.55 * percent_rank() over (order by b.revenue_window_php)
               + 0.20 * percent_rank() over (order by b.data_mb_window)
               + 0.10 * percent_rank() over (order by b.voice_min_window)
               + 0.15 * (1 - percent_rank() over (order by b.tickets_window)), 4) as engagement_score
    from base b
)
select
    s.*,
    decode(ntile(4) over (order by s.engagement_score desc, s.customer_id), 1, 'PLATINUM', 2, 'GOLD', 3, 'SILVER', 'BRONZE') as value_tier,
    width_bucket(s.engagement_score, 0, 1, 5)                   as engagement_quintile,
    ratio_to_report(s.revenue_window_php) over ()               as revenue_share,
    (select m from last_month)                                  as tier_month
from scored s
