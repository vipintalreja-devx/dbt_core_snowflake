{{ config(
    materialized='incremental',
    schema='gold',
    tags=['insights', 'p06', 'billing', 'collections'],
    incremental_strategy='delete+insert',
    unique_key='billing_month',
    cluster_by=['billing_month'],
    on_schema_change='append_new_columns'
) }}
-- P06 Collections: the dunning journey of every dunned invoice.
-- Purpose : orders each invoice's dunning actions (SMS reminder -> email notice -> service
--           barred) and its settlement into one event stream and reads the journey with
--           MATCH_RECOGNIZE: how far dunning went, whether and when the invoice was cured, and
--           invoices that were dunned although already paid (a process defect).
-- Grain   : invoice_id (invoices with at least one dunning action)
-- Sources : int_invoice_collection, slv_dunning_event
{%- set window_start = "dateadd('month', -2, " ~ f3_month_lower_bound('billing_month') ~ ")" %}
with inv as (
    select * from {{ ref('int_invoice_collection') }}
    where billing_month >= {{ window_start }} and dunning_actions > 0
),
events as (
    select d.invoice_id, 'DUNNING' as event_type, d.dunning_stage as stage, d.dunning_action as action,
           d.action_date as event_date, d.action_at_utc as event_ts, d.dunning_stage as event_order
    from {{ ref('slv_dunning_event') }} d
    where d.action_date >= {{ window_start }}
      and d.invoice_id in (select invoice_id from inv)
    union all
    select invoice_id, 'SETTLED', null, 'PAYMENT_SETTLED', settled_date,
           dateadd('hour', 12, settled_date::timestamp_ntz), 9
    from inv
    where settled_date is not null
),
journeys as (
    select *
    from events
    match_recognize (
        partition by invoice_id
        order by event_ts, event_order
        measures
            count(paid_early.*) > 0                                         as dunned_after_payment,
            first(dun.event_date)                                           as first_dunning_date,
            last(dun.event_date)                                            as last_dunning_date,
            count(dun.*)                                                    as dunning_actions,
            max(dun.stage)                                                  as max_stage,
            last(dun.action)                                                as last_action,
            count(cure.*) > 0                                               as is_cured,
            last(cure.event_date)                                           as cured_date
        one row per match
        pattern (^ paid_early? dun+ cure?)
        define
            paid_early as event_type = 'SETTLED',
            dun        as event_type = 'DUNNING',
            cure       as event_type = 'SETTLED'
    )
)
select
    i.invoice_id, i.account_id, i.region_id, i.billing_month, i.issue_date, i.due_date,
    i.invoice_amount_php, i.outstanding_php, i.late_fee_php,
    j.first_dunning_date, j.last_dunning_date, j.dunning_actions, j.max_stage, j.last_action,
    j.is_cured, j.cured_date,
    datediff('day', j.first_dunning_date, j.cured_date)                     as days_first_dunning_to_cure,
    j.dunned_after_payment,
    case when j.dunned_after_payment then 'DUNNED_AFTER_PAYMENT'
         when j.is_cured then 'CURED_AT_STAGE_' || j.max_stage
         else 'UNRESOLVED_AT_STAGE_' || j.max_stage end                     as journey_outcome,
    {{ dbt_loaded_at() }}                                                   as _dbt_loaded_at
from journeys j
join inv i on i.invoice_id = j.invoice_id
