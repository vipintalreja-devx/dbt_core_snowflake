{{ config(
    materialized='incremental',
    schema='gold',
    incremental_strategy='delete+insert',
    unique_key='churn_date',
    on_schema_change='append_new_columns',
    cluster_by=['churn_date'],
    tags=['insights', 'p02', 'churn']
) }}
-- P02 Churn root-cause attribution: one row per churn with the primary cause and the evidence.
-- Grain  : subscription_id x churn_date
-- Sources: int_p02_churn_events, int_p02_precursor_events
-- Method : 1. recency-weighted score per cause family (events closer to churn weigh more:
--             weight x (1 + (90 - days_before)/90));
--          2. MATCH_RECOGNIZE finds escalation journeys in the event stream:
--             NETWORK_SPIRAL  = POOR_NETWORK/OUTAGE ... then TICKET(s) ... (service never recovered)
--             BILLING_SPIRAL  = BILL_SHOCK or LATE_PAYMENT ... then SUSPENDED/LATE_PAYMENT
--             SERVICE_FAILURE = two or more tickets, at least one SLA breach
--          3. a matched journey decides the root cause; otherwise the highest-scoring family; no evidence
--             -> UNEXPLAINED (price / competitor / relocation, invisible in this data).
with ch as (
    select * from {{ ref('int_p02_churn_events') }}
    where churn_date >= {{ f1_lower_bound('churn_date') }}
),
ev as (
    select * from {{ ref('int_p02_precursor_events') }}
    where churn_date >= {{ f1_lower_bound('churn_date') }}
),
family as (
    select subscription_id, churn_date,
           case when event_type in ('POOR_NETWORK', 'OUTAGE') then 'NETWORK'
                when event_type in ('TICKET', 'TICKET_SLA_BREACH') then 'SERVICE'
                else 'BILLING' end                                       as cause_family,
           sum(weight * (1 + (90 - days_before_churn) / 90))             as score,
           sum(occurrences)                                              as events
    from ev group by 1, 2, 3
),
scores as (
    select subscription_id, churn_date,
           object_agg(cause_family, round(score, 2)::variant)            as cause_scores,
           max_by(cause_family, score)                                   as top_family,
           max(score)                                                    as top_score,
           sum(score)                                                    as total_score
    from family group by 1, 2
),
journeys as (
    select * from ev
    match_recognize (
        partition by subscription_id, churn_date
        order by event_date, event_type
        measures
            classifier()                                                 as last_step,
            match_number()                                               as mn,
            case when count(net.*) > 0 and count(tk.*) > 0 then 'NETWORK_SPIRAL'
                 when count(bill.*) > 0 and count(dist.*) > 0 then 'BILLING_SPIRAL'
                 else 'SERVICE_FAILURE' end                              as journey,
            first(event_date)                                            as journey_start,
            count(*)                                                     as journey_events
        one row per match
        after match skip past last row
        pattern ( (net+ tk+) | (bill+ dist+) | (tk tk+) )
        define
            net  as event_type in ('POOR_NETWORK', 'OUTAGE'),
            tk   as event_type in ('TICKET', 'TICKET_SLA_BREACH'),
            bill as event_type in ('BILL_SHOCK', 'LATE_PAYMENT'),
            dist as event_type in ('SUSPENDED', 'LATE_PAYMENT')
    )
),
best_journey as (
    select * from journeys
    qualify row_number() over (partition by subscription_id, churn_date
                               order by journey_events desc, journey_start desc) = 1
),
counts as (
    select subscription_id, churn_date,
           count_if(event_type = 'POOR_NETWORK')                         as poor_network_days,
           sum(iff(event_type = 'OUTAGE', occurrences, 0))               as outages,
           sum(iff(event_type in ('TICKET', 'TICKET_SLA_BREACH'), occurrences, 0)) as tickets,
           sum(iff(event_type = 'TICKET_SLA_BREACH', occurrences, 0))    as sla_breaches,
           count_if(event_type = 'BILL_SHOCK')                           as bill_shocks,
           sum(iff(event_type = 'LATE_PAYMENT', occurrences, 0))         as late_payments,
           count_if(event_type = 'SUSPENDED')                            as suspensions,
           min(days_before_churn)                                        as days_since_last_signal
    from ev group by 1, 2
)
select
    ch.subscription_id,
    ch.churn_date,
    date_trunc('month', ch.churn_date)::date                             as churn_month,
    ch.region_id, ch.region_name, ch.plan_id, ch.contract_type, ch.plan_category,
    ch.tenure_months, ch.avg_revenue_3m_php,
    coalesce(c.poor_network_days, 0) as poor_network_days, coalesce(c.outages, 0) as outages,
    coalesce(c.tickets, 0) as tickets, coalesce(c.sla_breaches, 0) as sla_breaches,
    coalesce(c.bill_shocks, 0) as bill_shocks, coalesce(c.late_payments, 0) as late_payments,
    coalesce(c.suspensions, 0) as suspensions, c.days_since_last_signal,
    j.journey                                                            as matched_journey,
    j.journey_start,
    s.cause_scores,
    case
        when j.journey = 'NETWORK_SPIRAL'  then 'NETWORK'
        when j.journey = 'BILLING_SPIRAL'  then 'BILLING'
        when j.journey = 'SERVICE_FAILURE' then 'SERVICE'
        when s.top_family is not null      then s.top_family
        else 'UNEXPLAINED' end                                           as root_cause,
    round(coalesce(s.top_score / nullif(s.total_score, 0), 0), 3)        as root_cause_confidence
from ch
left join counts c       on c.subscription_id = ch.subscription_id and c.churn_date = ch.churn_date
left join scores s       on s.subscription_id = ch.subscription_id and s.churn_date = ch.churn_date
left join best_journey j on j.subscription_id = ch.subscription_id and j.churn_date = ch.churn_date
