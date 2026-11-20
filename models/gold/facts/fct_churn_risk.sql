{{ config(
    materialized='incremental',
    schema='gold',
    unique_key=['subscription_id', 'score_date'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    cluster_by=['score_date', 'region_name'],
    tags=['gold', 'facts', 'churn', 'g2']
) }}
-- Rule-based churn-risk score per line and scoring date, blending the data-science model score
-- with behavioural signals (care contacts, recharge lapses, revenue trend, campaign fatigue,
-- network experience, overdue balance).
-- Ported from telco_v2_core g2_fct_churn_risk (PIPE.SP_GOLD_CHURN_RISK); repointed to silver and
-- scored as of the latest loaded business date (one new score_date per run).
-- Grain: subscription x score_date.
-- Sources (10): int_subscriber_profile, slv_churn_score, churn_features (bronze), slv_ticket,
-- slv_campaign_response, slv_recharge, slv_daily_revenue, fct_cell_kpi_daily, fct_invoice, slv_nps_survey.
with params as (
    select max(business_date) as score_date from {{ ref('slv_daily_revenue') }}
),
subs as (
    select subscription_id, account_id, plan_id, contract_type, home_cell_id, home_region_name as region_name, brand_name, tenure_months
    from {{ ref('int_subscriber_profile') }}
    where is_reportable and status in ('ACTIVE', 'SUSPENDED')
),
score as (
    select subscription_id, churn_probability, model_version from {{ ref('slv_churn_score') }}
),
features as (
    select f.subscription_id, f.tenure_months, f.avg_monthly_spend, f.data_trend_pct, f.late_payments_6m
    from {{ source('marketing', 'churn_features') }} f
    cross join params p
    where f.feature_date <= p.score_date
    qualify row_number() over (partition by f.subscription_id order by f.feature_date desc) = 1
),
tickets as (
    select t.subscription_id,
           count(*)                                             as tickets_90d,
           count_if(t.is_open)                                  as open_tickets,
           count_if(t.category in ('NO_INTERNET', 'SLOW_SPEED')) as network_tickets_90d,
           max_by(t.category, t.opened_at_utc)                  as last_ticket_category
    from {{ ref('slv_ticket') }} t
    cross join params p
    where t.opened_date between dateadd('day', -89, p.score_date) and p.score_date
    group by 1
),
campaigns as (
    select r.subscription_id, count(*) as campaigns_90d, count_if(r.is_converted) as conversions_90d,
           count_if(r.is_opened) as opens_90d
    from {{ ref('slv_campaign_response') }} r
    cross join params p
    where r.event_date between dateadd('day', -89, p.score_date) and p.score_date
    group by 1
),
recharge_gaps as (
    select r.subscription_id, r.recharged_at_local,
           datediff('day', lag(r.recharged_at_local) over (partition by r.subscription_id order by r.recharged_at_local),
                    r.recharged_at_local)                       as gap_days
    from {{ ref('slv_recharge') }} r
    cross join params p
    where r.event_date between dateadd('day', -179, p.score_date) and p.score_date
),
recharges as (
    select g.subscription_id,
           datediff('day', max(g.recharged_at_local), dateadd('day', 1, max(p.score_date))::timestamp_ntz) as days_since_recharge,
           max(g.gap_days)                                      as max_recharge_gap_days,
           max(g.lapse)                                         as recharge_lapses_180d
    from (select rg.*, conditional_true_event(rg.gap_days > 30)
                         over (partition by rg.subscription_id order by rg.recharged_at_local) as lapse
          from recharge_gaps rg) g
    cross join params p
    group by 1
),
revenue as (
    select r.subscription_id,
           sum(iff(r.business_date >  dateadd('day', -45, p.score_date), r.revenue_php, 0)) as revenue_last_45d_php,
           sum(iff(r.business_date <= dateadd('day', -45, p.score_date), r.revenue_php, 0)) as revenue_prev_45d_php
    from {{ ref('slv_daily_revenue') }} r
    cross join params p
    where r.business_date between dateadd('day', -89, p.score_date) and p.score_date
    group by 1
),
network as (
    -- home-cell experience over the last 30 days
    select k.cell_id, count_if(k.is_degraded) as degraded_days_30d, avg(k.drop_rate_pct_avg) as drop_rate_pct_30d
    from {{ ref('fct_cell_kpi_daily') }} k
    cross join params p
    where k.event_date between dateadd('day', -29, p.score_date) and p.score_date
    group by 1
),
balance as (
    select account_id, sum(outstanding_php) as overdue_php
    from {{ ref('fct_invoice') }} i
    cross join params p
    where not i.is_settled and i.due_date < p.score_date
    group by 1
),
nps as (
    select subscription_id, max_by(nps_score, responded_at_utc) as last_nps_score
    from {{ ref('slv_nps_survey') }}
    group by 1
),
signals as (
    select
        s.subscription_id, s.account_id, s.region_name, s.plan_id, s.contract_type, s.brand_name,
        sc.churn_probability, sc.model_version,
        coalesce(f.tenure_months, s.tenure_months)              as tenure_months,
        f.avg_monthly_spend, f.data_trend_pct, zeroifnull(f.late_payments_6m) as late_payments_6m,
        zeroifnull(t.tickets_90d)                               as tickets_90d,
        zeroifnull(t.open_tickets)                              as open_tickets,
        zeroifnull(t.network_tickets_90d)                       as network_tickets_90d,
        t.last_ticket_category,
        zeroifnull(cp.campaigns_90d)                            as campaigns_90d,
        zeroifnull(cp.conversions_90d)                          as conversions_90d,
        r.days_since_recharge,
        r.max_recharge_gap_days,
        zeroifnull(r.recharge_lapses_180d)                      as recharge_lapses_180d,
        zeroifnull(rv.revenue_last_45d_php)                     as revenue_last_45d_php,
        zeroifnull(rv.revenue_prev_45d_php)                     as revenue_prev_45d_php,
        round(div0(rv.revenue_last_45d_php - rv.revenue_prev_45d_php, rv.revenue_prev_45d_php) * 100, 2) as revenue_trend_pct,
        zeroifnull(n.degraded_days_30d)                         as home_cell_degraded_days_30d,
        n.drop_rate_pct_30d                                     as home_cell_drop_rate_pct_30d,
        zeroifnull(b.overdue_php)                               as account_overdue_php,
        np.last_nps_score,
        -- weighted risk components (sum to 1.0 at maximum)
        0.35 * zeroifnull(sc.churn_probability)                                              as p_model,
        0.12 * least(zeroifnull(t.tickets_90d) / 3, 1)                                       as p_tickets,
        0.12 * iff(coalesce(r.days_since_recharge, 999) > 30, 1, 0)                          as p_recharge,
        0.12 * iff(div0(rv.revenue_last_45d_php - rv.revenue_prev_45d_php, rv.revenue_prev_45d_php) < -0.2, 1, 0) as p_revenue,
        0.09 * iff(zeroifnull(cp.campaigns_90d) > 0 and zeroifnull(cp.conversions_90d) = 0, 1, 0) as p_campaign,
        0.10 * least(zeroifnull(n.degraded_days_30d) / 10, 1)                                as p_network,
        0.10 * iff(zeroifnull(b.overdue_php) > 0 or zeroifnull(f.late_payments_6m) >= 2, 1, 0) as p_billing
    from subs s
    left join score sc     on sc.subscription_id = s.subscription_id
    left join features f   on f.subscription_id = s.subscription_id
    left join tickets t    on t.subscription_id = s.subscription_id
    left join campaigns cp on cp.subscription_id = s.subscription_id
    left join recharges r  on r.subscription_id = s.subscription_id
    left join revenue rv   on rv.subscription_id = s.subscription_id
    left join network n    on n.cell_id = s.home_cell_id
    left join balance b    on b.account_id = s.account_id
    left join nps np       on np.subscription_id = s.subscription_id
    where rv.subscription_id is not null
),
scored as (
    select
        sg.*,
        round(p_model + p_tickets + p_recharge + p_revenue + p_campaign + p_network + p_billing, 4) as risk_score,
        case greatest(p_model, p_tickets, p_recharge, p_revenue, p_campaign, p_network, p_billing)
            when 0          then 'NONE'
            when p_model    then 'MODEL_SCORE'
            when p_tickets  then 'SUPPORT_TICKETS'
            when p_network  then 'NETWORK_EXPERIENCE'
            when p_billing  then 'BILLING_ISSUES'
            when p_recharge then 'NO_RECENT_RECHARGE'
            when p_revenue  then 'REVENUE_DROP'
            else 'IGNORES_CAMPAIGNS'
        end                                                     as main_reason,
        object_construct_keep_null(
            'model', round(p_model, 3), 'tickets', round(p_tickets, 3), 'recharge', round(p_recharge, 3),
            'revenue', round(p_revenue, 3), 'campaign', round(p_campaign, 3),
            'network', round(p_network, 3), 'billing', round(p_billing, 3))  as risk_components
    from signals sg
)
select
    sc.* exclude (p_model, p_tickets, p_recharge, p_revenue, p_campaign, p_network, p_billing),
    p.score_date,
    case when sc.risk_score >= 0.5 then 'HIGH' when sc.risk_score >= 0.3 then 'MEDIUM' else 'LOW' end as risk_band,
    width_bucket(sc.risk_score, 0, 1, 10)                       as risk_decile_bucket,
    percent_rank() over (partition by sc.region_name order by sc.risk_score) as risk_pct_rank_in_region,
    current_timestamp()                                         as scored_at
from scored sc
cross join params p
-- incremental: merge on (subscription_id, score_date) - re-running a date upserts, a new date appends
