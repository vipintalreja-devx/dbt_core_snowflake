{{ config(
    materialized='incremental',
    schema='gold',
    tags=['insights', 'p07', 'care', 'cost'],
    incremental_strategy='delete+insert',
    unique_key='month_start',
    cluster_by=['month_start', 'region_id'],
    on_schema_change='append_new_columns'
) }}
-- P07 Cost-to-serve: care cost vs revenue for every line and month.
-- Purpose : care cost (tickets, re-opens, notes, field work -- pivoted by cost type) against the
--           line's revenue; margin after care cost, cost-to-revenue ratio and an UNPROFITABLE flag.
--           Lines in base with no care activity appear with zero cost.
-- Grain   : month_start x subscription_id
-- Sources : int_care_cost_event, fct_subscriber_monthly, int_subscriber_profile
with cost as (
    select *
    from (select date_trunc('month', event_date)::date as month_start, subscription_id, cost_type, cost_php
          from {{ ref('int_care_cost_event') }}
          where {{ f2_month_window("date_trunc('month', event_date)::date", 'month_start') }})
    pivot (sum(cost_php) for cost_type in ('TICKET', 'REOPEN', 'NOTE', 'FIELD'))
        as p (month_start, subscription_id, ticket_cost_php, reopen_cost_php, note_cost_php, field_cost_php)
),
activity as (
    select date_trunc('month', event_date)::date as month_start, subscription_id,
           count_if(cost_type = 'TICKET') as tickets, count_if(cost_type = 'NOTE') as notes,
           count_if(cost_type = 'FIELD') as work_orders
    from {{ ref('int_care_cost_event') }}
    where {{ f2_month_window("date_trunc('month', event_date)::date", 'month_start') }}
    group by 1, 2
),
revenue as (
    select month_start, subscription_id, total_revenue_php, is_in_base_eom, is_churned, plan_id, contract_type
    from {{ ref('fct_subscriber_monthly') }}
    where {{ f2_month_window('month_start', 'month_start') }}
),
lines as (
    select coalesce(r.month_start, c.month_start)                           as month_start,
           coalesce(r.subscription_id, c.subscription_id)                   as subscription_id,
           r.total_revenue_php, r.is_in_base_eom, r.is_churned,
           c.ticket_cost_php, c.reopen_cost_php, c.note_cost_php, c.field_cost_php
    from revenue r
    full outer join cost c on c.month_start = r.month_start and c.subscription_id = r.subscription_id
)
select
    l.month_start, l.subscription_id,
    p.home_region_id                                                        as region_id,
    p.plan_category, p.contract_type, p.churn_band, p.is_mvno_line,
    case when p.tenure_months < 6 then '0-5m' when p.tenure_months < 12 then '6-11m'
         when p.tenure_months < 24 then '12-23m' else '24m+' end            as tenure_band,
    coalesce(l.is_in_base_eom, false)                                       as is_in_base_eom,
    coalesce(l.is_churned, false)                                           as is_churned,
    zeroifnull(a.tickets)                                                   as tickets,
    zeroifnull(a.notes)                                                     as notes,
    zeroifnull(a.work_orders)                                               as work_orders,
    round(zeroifnull(l.total_revenue_php), 2)                               as revenue_php,
    round(zeroifnull(l.ticket_cost_php), 2)                                 as ticket_cost_php,
    round(zeroifnull(l.reopen_cost_php), 2)                                 as reopen_cost_php,
    round(zeroifnull(l.note_cost_php), 2)                                   as note_cost_php,
    round(zeroifnull(l.field_cost_php), 2)                                  as field_cost_php,
    round(zeroifnull(l.ticket_cost_php) + zeroifnull(l.reopen_cost_php) + zeroifnull(l.note_cost_php) + zeroifnull(l.field_cost_php), 2) as care_cost_php,
    round(zeroifnull(l.total_revenue_php)
          - (zeroifnull(l.ticket_cost_php) + zeroifnull(l.reopen_cost_php) + zeroifnull(l.note_cost_php) + zeroifnull(l.field_cost_php)), 2) as margin_after_care_php,
    round(div0(zeroifnull(l.ticket_cost_php) + zeroifnull(l.reopen_cost_php) + zeroifnull(l.note_cost_php) + zeroifnull(l.field_cost_php),
               zeroifnull(l.total_revenue_php)), 3)                         as cost_to_revenue_ratio,
    (zeroifnull(l.total_revenue_php)
      - (zeroifnull(l.ticket_cost_php) + zeroifnull(l.reopen_cost_php) + zeroifnull(l.note_cost_php) + zeroifnull(l.field_cost_php))) < 0 as is_unprofitable,
    {{ dbt_loaded_at() }}                                                   as _dbt_loaded_at
from lines l
left join activity a on a.month_start = l.month_start and a.subscription_id = l.subscription_id
left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = l.subscription_id
