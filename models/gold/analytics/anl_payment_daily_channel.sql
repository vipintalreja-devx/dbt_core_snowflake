{{ config(
    materialized='table',
    cluster_by=['payment_date'],
    tags=['gold', 'analytics', 'billing', 'payments', 'm29']
) }}
-- Daily cash collection per payment channel / method: value, count, timeliness against due date,
-- share of the day, and a 7-day moving average for treasury forecasting.
-- Ported from x_m29_dt_payments_daily (was a dynamic table on BILLING.PAYMENT).
-- Grain: payment_date x payment_channel x payment_method.
-- Sources: fct_payment, dim_account, int_subscriber_profile.
with acct_region as (
    select account_id, any_value(island_group) as island_group
    from {{ ref('int_subscriber_profile') }} group by 1
),
daily as (
    select
        p.payment_date,
        p.payment_channel,
        coalesce(p.payment_method, 'UNKNOWN')               as payment_method,
        count(*)                                            as payments,
        count(distinct p.account_id)                        as paying_accounts,
        sum(p.payment_amount_php)                           as amount_php,
        count_if(p.is_late)                                 as late_payments,
        avg(p.days_after_due)                               as avg_days_after_due,
        count_if(a.is_paperless_billing)                    as paperless_payments,
        count_if(ar.island_group = 'MINDANAO')              as mindanao_payments
    from {{ ref('fct_payment') }} p
    left join {{ ref('dim_account') }} a on a.account_id = p.account_id
    left join acct_region ar             on ar.account_id = p.account_id
    where p.payment_date >= '{{ var("history_start_date") }}'::date
    group by 1, 2, 3
)
select
    d.*,
    round(div0(d.late_payments, d.payments), 4)             as late_share,
    round(ratio_to_report(d.amount_php) over (partition by d.payment_date), 4) as share_of_day_amount,
    round(avg(d.amount_php) over (partition by d.payment_channel, d.payment_method
                                  order by d.payment_date rows between 6 preceding and current row), 2) as amount_php_ma7,
    current_timestamp()                                     as loaded_at
from daily d
