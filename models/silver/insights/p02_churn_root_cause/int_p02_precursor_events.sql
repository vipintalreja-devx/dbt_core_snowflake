{{ config(
    materialized='incremental',
    schema='silver',
    incremental_strategy='delete+insert',
    unique_key='churn_date',
    on_schema_change='append_new_columns',
    cluster_by=['churn_date'],
    tags=['insights', 'p02', 'churn']
) }}
-- P02 step 2: everything that happened to a churned line in the 90 days before it left, as one
-- ordered event stream.
-- Grain  : subscription_id x churn_date x event_date x event_type
-- Sources: int_p02_churn_events, fct_ticket (tickets, re-opens, SLA breaches), fct_p01_experience_daily
--          (POOR network days), slv_outage_impact + fct_outage (outages), fct_subscriber_monthly (bill
--          shock: revenue +30% m/m), fct_invoice (late / unpaid invoices of the account),
--          slv_subscription_status_history (suspensions)
-- Event types: TICKET, TICKET_SLA_BREACH, POOR_NETWORK, OUTAGE, BILL_SHOCK, LATE_PAYMENT, SUSPENDED
with ch as (
    select * from {{ ref('int_p02_churn_events') }}
    where churn_date >= {{ f1_lower_bound('churn_date') }}
),
events as (
    select ch.subscription_id, ch.churn_date, t.opened_date as event_date,
           iff(t.is_within_sla = false, 'TICKET_SLA_BREACH', 'TICKET') as event_type,
           1 + coalesce(t.reopen_count, 0)                              as weight,
           t.category                                                   as detail
    from ch join {{ ref('fct_ticket') }} t on t.subscription_id = ch.subscription_id
    where t.opened_date between dateadd('day', -90, ch.churn_date) and ch.churn_date
    union all
    select ch.subscription_id, ch.churn_date, e.event_date, 'POOR_NETWORK', 1, e.experience_score::varchar
    from ch join {{ ref('fct_p01_experience_daily') }} e on e.subscription_id = ch.subscription_id
    where e.event_date between dateadd('day', -90, ch.churn_date) and ch.churn_date
      and e.experience_band = 'POOR'
    union all
    select ch.subscription_id, ch.churn_date, o.event_date, 'OUTAGE', least(3, ceil(i.impact_minutes / 60)), o.root_cause
    from ch
    join {{ ref('slv_outage_impact') }} i on i.subscription_id = ch.subscription_id
    join {{ ref('fct_outage') }} o on o.incident_id = i.incident_id
    where o.event_date between dateadd('day', -90, ch.churn_date) and ch.churn_date
    union all
    select ch.subscription_id, ch.churn_date, last_day(m.month_start), 'BILL_SHOCK', 2,
           round(m.total_revenue_php, 2) || ' vs ' || round(m.prev_rev, 2)
    from ch
    join (select subscription_id, month_start, total_revenue_php,
                 lag(total_revenue_php) over (partition by subscription_id order by month_start) as prev_rev
            from {{ ref('fct_subscriber_monthly') }}
           where month_start >= dateadd('month', -5, (select min(churn_date) from ch))) m
      on m.subscription_id = ch.subscription_id
    where last_day(m.month_start) between dateadd('day', -90, ch.churn_date) and ch.churn_date
      and m.prev_rev > 0 and m.total_revenue_php > m.prev_rev * 1.3
    union all
    select ch.subscription_id, ch.churn_date, i.due_date, 'LATE_PAYMENT', iff(i.is_settled, 1, 2), i.invoice_status
    from ch join {{ ref('fct_invoice') }} i on i.account_id = ch.account_id
    where i.due_date between dateadd('day', -90, ch.churn_date) and ch.churn_date
      and coalesce(i.is_paid_on_time, false) = false
    union all
    select ch.subscription_id, ch.churn_date, h.valid_from_date, 'SUSPENDED', 2, null
    from ch join {{ ref('slv_subscription_status_history') }} h on h.subscription_id = ch.subscription_id
    where h.status = 'SUSPENDED' and h.valid_from_date between dateadd('day', -90, ch.churn_date) and ch.churn_date
)
select subscription_id, churn_date, event_date, event_type,
       sum(weight)                                                      as weight,
       count(*)                                                         as occurrences,
       any_value(detail)                                                as detail,
       datediff('day', event_date, churn_date)                          as days_before_churn
from events
group by 1, 2, 3, 4
