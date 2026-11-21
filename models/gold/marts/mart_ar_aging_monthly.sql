{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='month_end',
    on_schema_change='append_new_columns',
    schema='gold',
    tags=['gold', 'marts', 'kpi'],
    transient=false,
    copy_grants=true
) }}
-- Accounts-receivable aging at each month end (replaces PIPE_X.SP_X_M04_INVOICE_AGING_MONTHLY,
-- but for every month in one set-based statement instead of one CALL per month).
-- Each invoice's settlement date is computed once; only invoices still open at a month end
-- are carried, and their paid-to-date is resolved with an ASOF join.
with month_ends as (
    select distinct month_end
    from {{ ref('dim_date') }}
    where date_day between '{{ var("history_start_date") }}'::date
                       and (select max(issue_date) from {{ ref('slv_invoice') }})
      -- incremental: restate the last two month ends (late payments / credits / reversals)
      and month_end >= {{ f3_month_lower_bound('month_end') }}
),
inv as (
    select invoice_id, account_id, issue_date, due_date, invoice_amount_php
    from {{ ref('slv_invoice') }}
),
events as (
    select invoice_id, payment_date as event_date, payment_amount_php as amount_php
    from {{ ref('slv_payment') }} where invoice_id is not null
    union all
    select invoice_id, adjustment_date, credit_amount_php
    from {{ ref('slv_adjustment') }} where invoice_id is not null
),
events_daily as (
    select invoice_id, event_date, sum(amount_php) as amount_php
    from events group by 1, 2
),
events_cum as (
    select invoice_id, event_date,
           sum(amount_php) over (partition by invoice_id order by event_date
                                 rows between unbounded preceding and current row) as settled_to_date_php
    from events_daily
),
settlement as (
    select c.invoice_id, min(c.event_date) as settled_date
    from events_cum c
    join inv i on i.invoice_id = c.invoice_id
    where c.settled_to_date_php >= i.invoice_amount_php - 0.005
    group by 1
),
open_at_month_end as (
    select me.month_end, i.*
    from month_ends me
    join inv i on i.issue_date <= me.month_end
    left join settlement s on s.invoice_id = i.invoice_id
    where s.settled_date is null or s.settled_date > me.month_end
),
balances as (
    select
        o.month_end,
        o.invoice_id,
        o.account_id,
        datediff('day', o.due_date, o.month_end)            as days_past_due,
        o.invoice_amount_php - zeroifnull(c.settled_to_date_php) as outstanding_php
    from open_at_month_end o
    asof join events_cum c
        match_condition (o.month_end >= c.event_date)
        on o.invoice_id = c.invoice_id
)
select
    month_end,
    case when days_past_due <= 0 then '0 CURRENT' when days_past_due <= 30 then '1-30'
         when days_past_due <= 60 then '31-60' when days_past_due <= 90 then '61-90' else '90+' end as aging_bucket,
    count(*)                                                as open_invoices,
    count(distinct account_id)                              as accounts,
    round(sum(outstanding_php), 2)                          as outstanding_php
from balances
where outstanding_php > 0.005
group by 1, 2
