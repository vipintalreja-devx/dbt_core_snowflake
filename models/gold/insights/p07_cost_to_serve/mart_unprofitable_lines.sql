{{ config(
    materialized='table',
    schema='gold',
    tags=['insights', 'p07', 'care', 'cost', 'mart'],
    transient=false,
    cluster_by=['region_id'],
    copy_grants=true
) }}
-- P07 Cost-to-serve: lines that cost more to serve than they earn (current state).
-- Purpose : over the last three months, lines whose care cost exceeds their revenue, with the
--           dominant cost driver, rank within region and a suggested action.
-- Grain   : subscription_id (unprofitable lines only)
-- Sources : fct_line_cost_to_serve_monthly
with last3 as (
    select *
    from {{ ref('fct_line_cost_to_serve_monthly') }}
    where month_start >= dateadd('month', -2, (select max(month_start) from {{ ref('fct_line_cost_to_serve_monthly') }}))
),
agg as (
    select subscription_id,
           max_by(region_id, month_start)                                   as region_id,
           max_by(plan_category, month_start)                               as plan_category,
           max_by(churn_band, month_start)                                  as churn_band,
           max_by(tenure_band, month_start)                                 as tenure_band,
           max(month_start)                                                 as last_month,
           sum(revenue_php)                                                 as revenue_3m_php,
           sum(care_cost_php)                                               as care_cost_3m_php,
           sum(ticket_cost_php)                                             as ticket_cost_3m_php,
           sum(reopen_cost_php)                                             as reopen_cost_3m_php,
           sum(note_cost_php)                                               as note_cost_3m_php,
           sum(field_cost_php)                                              as field_cost_3m_php,
           sum(tickets)                                                     as tickets_3m,
           sum(work_orders)                                                 as work_orders_3m
    from last3
    group by 1
)
select
    a.*,
    round(revenue_3m_php - care_cost_3m_php, 2)                             as margin_after_care_3m_php,
    case greatest(ticket_cost_3m_php, reopen_cost_3m_php, note_cost_3m_php, field_cost_3m_php)
         when field_cost_3m_php  then 'FIELD_WORK'
         when ticket_cost_3m_php then 'TICKET_HANDLING'
         when note_cost_3m_php   then 'FOLLOW_UP_NOTES'
         else 'REOPENS' end                                                 as top_cost_driver,
    rank() over (partition by region_id order by revenue_3m_php - care_cost_3m_php)  as loss_rank_in_region,
    case when work_orders_3m >= 2 then 'ROOT-CAUSE FIELD ISSUE (repeat visits)'
         when tickets_3m >= 3 then 'PROACTIVE OUTREACH / FIX RECURRING ISSUE'
         when churn_band = 'HIGH' then 'RETENTION REVIEW BEFORE FURTHER SPEND'
         else 'MOVE TO DIGITAL SELF-SERVICE' end                            as suggested_action
from agg a
where revenue_3m_php - care_cost_3m_php < 0
