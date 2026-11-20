{{ config(
    materialized='dynamic_table',
    target_lag='1 hour',
    snowflake_warehouse='COMPUTE_WH',
    refresh_mode='auto',
    tags=['gold', 'analytics', 'care', 'backlog', 'm30']
) }}
-- Open care-ticket backlog per category, priority and month opened, with age buckets - a Snowflake
-- DYNAMIC TABLE that keeps itself within 1 hour of slv_ticket without a dbt run.
-- Ported from x_m30_dt_ticket_backlog (dynamic table on CARE.TICKET).
-- Grain: category x priority x open_month.
-- Sources: slv_ticket, slv_subscription, sla_policy (seed).
select
    t.category,
    t.priority,
    date_trunc('month', t.opened_date)::date                as open_month,
    count(*)                                                as open_tickets,
    count(distinct t.subscription_id)                       as affected_lines,
    count_if(s.is_mvno_line)                                as mvno_tickets,
    count_if(datediff('hour', t.opened_at_utc, sysdate()) > sla.resolution_sla_hours) as breaching_sla,
    count_if(datediff('day', t.opened_date, current_date()) <= 7)  as age_0_7d,
    count_if(datediff('day', t.opened_date, current_date()) between 8 and 30) as age_8_30d,
    count_if(datediff('day', t.opened_date, current_date()) > 30)  as age_over_30d,
    max(sla.resolution_sla_hours)                           as resolution_sla_hours
from {{ ref('slv_ticket') }} t
left join {{ ref('slv_subscription') }} s on s.subscription_id = t.subscription_id
left join {{ ref('sla_policy') }} sla     on sla.priority = t.priority
where t.is_open
group by 1, 2, 3
