{{ config(
    materialized='incremental',
    schema='gold',
    tags=['insights', 'p06', 'billing', 'collections', 'mart'],
    incremental_strategy='delete+insert',
    unique_key='billing_month',
    cluster_by=['billing_month'],
    on_schema_change='append_new_columns'
) }}
-- P06 Collections: cure funnel per billing month, dunning stage and region.
-- Purpose : stage 0 = all invoices of the month (on-time rate, days to pay); stage 1-3 =
--           invoices that reached that dunning stage, how many were cured there, the cure rate,
--           days from first dunning to cure, amount still outstanding and late fees.
-- Grain   : billing_month x dunning_stage (0-3) x region_id
-- Sources : int_invoice_collection, fct_dunning_journey
{%- set window_start = "dateadd('month', -2, " ~ f3_month_lower_bound('billing_month') ~ ")" %}
with inv as (
    select i.*, j.is_cured, j.max_stage, j.days_first_dunning_to_cure, j.dunned_after_payment
    from {{ ref('int_invoice_collection') }} i
    left join {{ ref('fct_dunning_journey') }} j on j.invoice_id = i.invoice_id
    where i.billing_month >= {{ window_start }}
),
stages as (
    select column1 as dunning_stage from values (0), (1), (2), (3)
)
select
    i.billing_month, s.dunning_stage, i.region_id,
    count(*)                                                                as invoices,
    round(sum(i.invoice_amount_php), 2)                                     as billed_php,
    count_if(s.dunning_stage = 0 and i.collection_state = 'PAID_ON_TIME')   as paid_on_time,
    count_if(s.dunning_stage > 0 and i.is_cured and i.max_stage = s.dunning_stage) as cured_at_stage,
    round(div0(count_if(s.dunning_stage > 0 and i.is_cured and i.max_stage = s.dunning_stage), count(*)) * 100, 2) as cure_rate_pct,
    round(div0(count_if(i.collection_state = 'PAID_ON_TIME'), count(*)) * 100, 2) as on_time_rate_pct,
    round(avg(i.days_to_settle), 1)                                         as avg_days_to_settle,
    round(avg(iff(s.dunning_stage > 0, i.days_first_dunning_to_cure, null)), 1) as avg_days_dunning_to_cure,
    round(sum(i.outstanding_php), 2)                                        as outstanding_php,
    round(sum(i.late_fee_php), 2)                                           as late_fees_php,
    count_if(i.dunned_after_payment)                                        as dunned_after_payment,
    {{ dbt_loaded_at() }}                                                   as _dbt_loaded_at
from inv i
join stages s on s.dunning_stage = 0 or zeroifnull(i.max_dunning_stage) >= s.dunning_stage
group by 1, 2, 3
