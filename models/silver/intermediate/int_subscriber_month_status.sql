{{ config(
    tags=['silver', 'intermediate'],
    schema='silver',
    materialized='table') }}
-- Status of every line at the start and end of every month in the reporting window,
-- derived from the BSS status history. Basis for subscriber base, gross adds and churn.
with as_of as (
    -- data-driven reporting horizon: the last day the BSS has status events for
    select max(valid_from_date) as as_of_date from {{ ref('slv_subscription_status_history') }}
),
month_seq as (
    select row_number() over (order by seq4()) - 1 as n from table(generator(rowcount => 600))
),
months as (
    select
        dateadd('month', n, date_trunc('month', '{{ var("history_start_date") }}'::date))           as month_start,
        last_day(dateadd('month', n, date_trunc('month', '{{ var("history_start_date") }}'::date))) as month_end
    from month_seq
    where dateadd('month', n, date_trunc('month', '{{ var("history_start_date") }}'::date)) <= (select as_of_date from as_of)
),
h as (
    select * from {{ ref('slv_subscription_status_history') }}
),
bom as (
    select m.month_start, h.subscription_id, h.status as status_bom, h.is_in_base as in_base_bom
    from months m
    join h on m.month_start::timestamp_ntz >= h.valid_from_local
          and m.month_start::timestamp_ntz <  h.valid_to_local
),
eom as (
    select m.month_start, h.subscription_id, h.status as status_eom, h.is_in_base as in_base_eom
    from months m
    join h on dateadd('day', 1, m.month_end)::timestamp_ntz >  h.valid_from_local
          and dateadd('day', 1, m.month_end)::timestamp_ntz <= h.valid_to_local
),
moves as (
    -- status transitions that happened inside the month
    select
        date_trunc('month', h.valid_from_date)              as month_start,
        h.subscription_id,
        max(iff(h.status = 'CHURNED', 1, 0))                as churn_event,
        max(iff(h.status_seq = 1 and h.is_in_base, 1, 0))   as activation_event
    from h
    where h.valid_from_date >= date_trunc('month', '{{ var("history_start_date") }}'::date)
    group by 1, 2
)
select
    coalesce(e.month_start, b.month_start)                  as month_start,
    coalesce(e.subscription_id, b.subscription_id)          as subscription_id,
    b.status_bom,
    e.status_eom,
    coalesce(b.in_base_bom, false)                          as is_in_base_bom,
    coalesce(e.in_base_eom, false)                          as is_in_base_eom,
    coalesce(mv.activation_event, 0) = 1
        or (not coalesce(b.in_base_bom, false) and coalesce(e.in_base_eom, false)) as is_gross_add,
    coalesce(b.in_base_bom, false) and e.status_eom = 'CHURNED' as is_churned
from eom e
full outer join bom b
       on b.month_start = e.month_start and b.subscription_id = e.subscription_id
left join moves mv
       on mv.month_start = coalesce(e.month_start, b.month_start)
      and mv.subscription_id = coalesce(e.subscription_id, b.subscription_id)
