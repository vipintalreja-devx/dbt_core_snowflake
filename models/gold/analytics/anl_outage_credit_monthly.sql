{{ config(
    materialized='table',
    cluster_by=['outage_month'],
    tags=['gold', 'analytics', 'network', 'billing', 'sla_credit', 'm17']
) }}
-- Outage service-credit liability per account and month: credit owed (pro-rata of the monthly
-- charge for credit-eligible outage minutes) vs. OUTAGE_CREDIT adjustments actually granted.
-- Ported from x_m17_outage_credit_monthly (PIPE_X.SP_X_M17_OUTAGE_CREDIT_MONTHLY).
-- Grain: outage_month x account_id.
-- Sources: slv_outage_incident, slv_outage_impact, slv_subscription, slv_adjustment, slv_region.
with impact as (
    select
        date_trunc('month', i.event_date)::date             as outage_month,
        s.account_id,
        i.incident_id,
        i.severity,
        coalesce(r.region_name, 'UNKNOWN')                  as region_name,
        im.subscription_id,
        im.impact_minutes,
        s.monthly_charges_php,
        round(s.monthly_charges_php * im.impact_minutes
              / (datediff('day', date_trunc('month', i.event_date), last_day(i.event_date)) + 1) / 1440, 4) as credit_owed_php
    from {{ ref('slv_outage_incident') }} i
    join {{ ref('slv_outage_impact') }} im on im.incident_id = i.incident_id and im.is_credit_eligible
    join {{ ref('slv_subscription') }} s   on s.subscription_id = im.subscription_id and s.is_reportable
    left join {{ ref('slv_region') }} r    on r.region_id = i.region_id
),
granted as (
    select account_id, date_trunc('month', adjustment_date)::date as credit_month,
           sum(credit_amount_php) as credit_granted_php, count(*) as credit_adjustments
    from {{ ref('slv_adjustment') }}
    where reason_code = 'OUTAGE_CREDIT'
    group by 1, 2
),
owed as (
    select
        outage_month,
        account_id,
        any_value(region_name)                              as region_name,
        count(distinct incident_id)                         as incidents,
        count(distinct subscription_id)                     as impacted_lines,
        max(severity)                                       as worst_severity_code,
        sum(impact_minutes)                                 as impact_minutes,
        sum(credit_owed_php)                                as credit_owed_php,
        array_agg(distinct incident_id) within group (order by incident_id) as incident_ids
    from impact
    group by 1, 2
)
select
    o.outage_month,
    o.account_id,
    o.region_name,
    o.incidents,
    o.impacted_lines,
    o.worst_severity_code,
    o.impact_minutes,
    round(o.credit_owed_php, 2)                             as credit_owed_php,
    round(zeroifnull(g.credit_granted_php), 2)              as credit_granted_php,
    zeroifnull(g.credit_adjustments)                        as credit_adjustments,
    round(o.credit_owed_php - zeroifnull(g.credit_granted_php), 2) as credit_gap_php,
    case
        when g.credit_granted_php is null                        then 'NOT_CREDITED'
        when g.credit_granted_php + 0.01 < o.credit_owed_php     then 'UNDER_CREDITED'
        when g.credit_granted_php > o.credit_owed_php * 1.5 + 1  then 'OVER_CREDITED'
        else 'CREDITED'
    end                                                     as credit_status,
    o.incident_ids,
    current_timestamp()                                     as loaded_at
from owed o
left join granted g on g.account_id = o.account_id and g.credit_month = o.outage_month
