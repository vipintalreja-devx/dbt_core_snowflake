{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='action_date',
    on_schema_change='append_new_columns',
    cluster_by=['action_date'],
    tags=['gold', 'aggregates', 'billing', 'collections', 'e05']
) }}
-- Dunning (collections) actions per day x stage x action x region, with cure outcome of the invoice.
-- Ported from x_e05_dunning_stage_daily (PIPE_X.SP_X_E05_DUNNING_STAGE_DAILY).
-- Grain   : action_date x dunning_stage x dunning_action x region_name
-- Sources : slv_dunning_event, fct_invoice, int_subscriber_profile, dim_region
with acct_region as (
    select account_id, max_by(home_region_id, monthly_charges_php) as region_id,
           max(iff(churn_band = 'HIGH', 1, 0)) = 1          as has_high_churn_line
    from {{ ref('int_subscriber_profile') }}
    group by 1
),
ev as (
    select
        d.dunning_id, d.account_id, d.invoice_id, d.dunning_stage, d.dunning_action, d.action_date,
        d.amount_due_php,
        f.is_settled,
        f.last_payment_date,
        iff(f.last_payment_date >= d.action_date, datediff('day', d.action_date, f.last_payment_date), null) as days_to_cure,
        coalesce(r.region_name, 'UNKNOWN')                  as region_name,
        coalesce(ar.has_high_churn_line, false)             as has_high_churn_line
    from {{ ref('slv_dunning_event') }} d
    left join {{ ref('fct_invoice') }} f on f.invoice_id = d.invoice_id
    left join acct_region ar            on ar.account_id = d.account_id
    left join {{ ref('dim_region') }} r on r.region_id = ar.region_id
    where d.action_date >= {{ f1_lower_bound('action_date', 7) }}
),
daily as (
    select
        action_date, dunning_stage, dunning_action, region_name,
        count(*)                                            as events,
        approx_count_distinct(account_id)                   as accounts_approx,
        sum(amount_due_php)                                 as amount_due_php,
        median(amount_due_php)                              as median_amount_due_php,
        count_if(is_settled)                                as invoices_cured,
        median(days_to_cure)                                as median_days_to_cure,
        count_if(dunning_action = 'SERVICE_BARRED')         as barrings,
        count_if(has_high_churn_line)                       as events_high_churn_accounts
    from ev
    group by 1, 2, 3, 4
)
select
    d.*,
    round(div0(d.invoices_cured, d.events) * 100, 2)        as cure_rate_pct,
    round(ratio_to_report(d.amount_due_php) over (partition by d.action_date), 6) as share_of_day_amount_due,
    sum(d.events) over (partition by d.dunning_stage, d.dunning_action, d.region_name order by d.action_date
                        rows between 6 preceding and current row) as events_7d,
    current_timestamp()                                     as loaded_at
from daily d
qualify d.action_date >= {{ f1_lower_bound('action_date') }}
