{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'marts', 'kpi'],
    transient=false,
    copy_grants=true
) }}
-- Materialization: table (current-state/small reference): defined as the current state of each subscriber.
-- One row per line: everything Care, Marketing and Retention need on one screen.
with last_month as (select max(month_start) as m from {{ ref('fct_subscriber_monthly') }}),
rev as (
    select subscription_id,
           sum(total_revenue_php)                           as revenue_window_php,
           avg(total_revenue_php)                           as avg_monthly_revenue_php,
           sum(iff(month_start = (select m from last_month), total_revenue_php, 0)) as revenue_last_month_php,
           sum(iff(month_start = (select m from last_month), data_mb, 0)) / 1024    as data_gb_last_month,
           sum(iff(month_start = (select m from last_month), voice_min, 0))         as voice_min_last_month
    from {{ ref('fct_subscriber_monthly') }}
    group by 1
),
tk as (
    select subscription_id,
           count_if(opened_date >= dateadd('day', -90, (select max(opened_date) from {{ ref('fct_ticket') }}))) as tickets_90d,
           count_if(is_open)                                as open_tickets,
           max_by(category, opened_at_local)                as last_ticket_category,
           max(opened_date)                                 as last_ticket_date
    from {{ ref('fct_ticket') }}
    group by 1
),
rc as (
    select subscription_id, max(event_date) as last_recharge_date, count(*) as recharges_window
    from {{ ref('fct_recharge') }}
    group by 1
),
nps as (
    select subscription_id, max_by(nps_score, responded_at_utc) as last_nps_score
    from {{ ref('slv_nps_survey') }}
    group by 1
),
ar as (
    select a.account_id, sum(i.outstanding_php) as account_outstanding_php
    from {{ ref('fct_invoice') }} i join {{ ref('dim_account') }} a on a.account_id = i.account_id
    where not i.is_settled
    group by 1
)
select
    d.subscription_id,
    d.account_id,
    d.customer_id,
    d.msisdn_masked,
    d.status,
    d.plan_name,
    d.contract_type,
    d.home_region_name,
    d.brand_name,
    d.segment_code,
    d.tenure_months,
    d.tenure_band,
    d.device_manufacturer,
    d.device_supports_5g,
    d.contract_end_date,
    datediff('day', current_date, d.contract_end_date)     as days_to_contract_end,
    round(zeroifnull(r.revenue_window_php), 2)              as revenue_window_php,
    round(zeroifnull(r.avg_monthly_revenue_php), 2)         as avg_monthly_revenue_php,
    round(zeroifnull(r.revenue_last_month_php), 2)          as revenue_last_month_php,
    round(zeroifnull(r.data_gb_last_month), 3)              as data_gb_last_month,
    round(zeroifnull(r.voice_min_last_month), 1)            as voice_min_last_month,
    zeroifnull(t.tickets_90d)                               as tickets_90d,
    zeroifnull(t.open_tickets)                              as open_tickets,
    t.last_ticket_category,
    t.last_ticket_date,
    rc.last_recharge_date,
    n.last_nps_score,
    round(zeroifnull(ar.account_outstanding_php), 2)        as account_outstanding_php,
    d.churn_probability,
    d.churn_band,
    case
        when d.churn_band = 'HIGH' and zeroifnull(t.open_tickets) > 0          then 'SERVICE_RECOVERY'
        when d.churn_band = 'HIGH' and zeroifnull(ar.account_outstanding_php) > 0 then 'COLLECTIONS_RISK'
        when d.churn_band in ('HIGH', 'MEDIUM')
             and datediff('day', current_date, d.contract_end_date) between 0 and 90 then 'RENEWAL_OFFER'
        when d.churn_band = 'HIGH'                                               then 'RETENTION_CALL'
        else 'NONE'
    end                                                     as next_best_action
from {{ ref('dim_subscriber') }} d
left join rev r  on r.subscription_id  = d.subscription_id
left join tk  t  on t.subscription_id  = d.subscription_id
left join rc     on rc.subscription_id = d.subscription_id
left join nps n  on n.subscription_id  = d.subscription_id
left join ar     on ar.account_id      = d.account_id
where d.is_reportable
