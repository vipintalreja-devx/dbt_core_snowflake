{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='payment_date',
    on_schema_change='append_new_columns',
    cluster_by=['payment_date'],
    tags=['gold', 'aggregates', 'billing', 'collections', 'e02']
) }}
-- Cash collected per Manila business day x payment channel x method, with punctuality vs. due date.
-- Ported from x_e02_payment_daily_method (PIPE_X.SP_X_E02_PAYMENT_DAILY_METHOD).
-- Grain   : payment_date x payment_channel x payment_method
-- Sources : slv_payment, slv_invoice, int_subscriber_profile, dim_date
with acct_region as (
    select account_id,
           max_by(island_group, monthly_charges_php)        as island_group
    from {{ ref('int_subscriber_profile') }}
    group by 1
),
pay as (
    select
        p.payment_id, p.account_id, p.payment_date, p.paid_at_local, p.payment_channel,
        coalesce(p.payment_method, 'UNKNOWN')               as payment_method,
        p.payment_amount_php,
        i.due_date,
        i.billing_month,
        datediff('day', i.due_date, p.payment_date)         as days_after_due,
        coalesce(a.island_group, 'UNKNOWN')                 as island_group
    from {{ ref('slv_payment') }} p
    left join {{ ref('slv_invoice') }} i on i.invoice_id = p.invoice_id
    left join acct_region a             on a.account_id = p.account_id
    where p.payment_date >= {{ f1_lower_bound('payment_date', 7) }}
),
island as (
    select payment_date, payment_channel, payment_method, island_group, sum(payment_amount_php) as amount_php
    from pay group by 1, 2, 3, 4
),
island_obj as (
    select payment_date, payment_channel, payment_method,
           object_agg(island_group, round(amount_php, 2)::variant) as amount_by_island
    from island group by 1, 2, 3
),
daily as (
    select
        payment_date, payment_channel, payment_method,
        count(*)                                            as payments,
        approx_count_distinct(account_id)                   as accounts_approx,
        sum(payment_amount_php)                             as amount_php,
        round(avg(payment_amount_php), 2)                   as avg_payment_php,
        median(payment_amount_php)                          as median_payment_php,
        approx_percentile(payment_amount_php, 0.9)          as p90_payment_php,
        count_if(days_after_due > 0)                        as late_payments,
        count_if(days_after_due <= 0)                       as on_time_payments,
        round(avg(iff(days_after_due > 0, days_after_due, null)), 1) as avg_days_late,
        count_if(due_date is null)                          as unmatched_payments,
        count_if(hour(paid_at_local) between 0 and 5)       as overnight_payments
    from pay
    group by 1, 2, 3
)
select
    d.*,
    o.amount_by_island,
    dd.day_name,
    dd.is_weekend,
    round(div0(d.late_payments, d.payments) * 100, 2)       as late_payment_pct,
    round(ratio_to_report(d.amount_php) over (partition by d.payment_date), 6) as share_of_day_amount,
    round(avg(d.amount_php) over (partition by d.payment_channel, d.payment_method order by d.payment_date
                                  rows between 6 preceding and current row), 2) as amount_php_7d_avg,
    lag(d.amount_php) over (partition by d.payment_channel, d.payment_method order by d.payment_date) as prev_day_amount_php,
    current_timestamp()                                     as loaded_at
from daily d
left join island_obj o
       on o.payment_date = d.payment_date and o.payment_channel = d.payment_channel and o.payment_method = d.payment_method
left join {{ ref('dim_date') }} dd on dd.date_day = d.payment_date
qualify d.payment_date >= {{ f1_lower_bound('payment_date') }}
