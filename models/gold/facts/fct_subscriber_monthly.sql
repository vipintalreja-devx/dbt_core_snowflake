{{ config(
    tags=['gold', 'facts', 'fact'],
    schema='gold',
    materialized='incremental',
    unique_key='month_start',
    incremental_strategy='delete+insert',
    cluster_by=['month_start'],
    on_schema_change='append_new_columns'
) }}
-- Grain: line x calendar month. The monthly subscriber KPI base: status movement,
-- usage, earned revenue by stream, recharges and care contacts.
-- Replaces PIPE.SP_GOLD_SUBSCRIBER_MONTHLY_KPI / PIPE_X month-level procedures.
{% set month_filter %}
    {%- if is_incremental() -%}
        >= (select dateadd('month', -1, max(month_start)) from {{ this }})
    {%- else -%}
        >= date_trunc('month', '{{ var("history_start_date") }}'::date)
    {%- endif -%}
{% endset %}
with status as (
    select * from {{ ref('int_subscriber_month_status') }}
    where month_start {{ month_filter }}
),
usage as (
    select subscription_id, date_trunc('month', event_date)::date as month_start,
           sum(voice_calls) as voice_calls, sum(voice_min) as voice_min, sum(sms_count) as sms_count,
           sum(data_mb) as data_mb, sum(data_mb_5g) as data_mb_5g, count(*) as active_days
    from {{ ref('fct_subscriber_usage_daily') }}
    where date_trunc('month', event_date) {{ month_filter }}
    group by 1, 2
),
revenue as (
    select subscription_id, date_trunc('month', business_date)::date as month_start,
           sum(plan_revenue_php) as plan_revenue_php, sum(recharge_revenue_php) as recharge_revenue_php,
           sum(vas_revenue_php) as vas_revenue_php, sum(roaming_revenue_php) as roaming_revenue_php,
           sum(total_revenue_php) as total_revenue_php
    from {{ ref('fct_revenue_daily') }}
    where date_trunc('month', business_date) {{ month_filter }}
    group by 1, 2
),
recharges as (
    select subscription_id, date_trunc('month', event_date)::date as month_start,
           count(*) as recharges, sum(recharge_amount_php) as recharge_amount_php
    from {{ ref('fct_recharge') }}
    where date_trunc('month', event_date) {{ month_filter }}
    group by 1, 2
),
tickets as (
    select subscription_id, date_trunc('month', opened_date)::date as month_start, count(*) as tickets
    from {{ ref('fct_ticket') }}
    where date_trunc('month', opened_date) {{ month_filter }}
    group by 1, 2
),
spine as (
    -- only closed reporting months: a month must exist in the BSS status spine. This keeps a
    -- partial month (e.g. CDRs of 30-Sep UTC that fall on 1-Oct Manila time) out of KPIs.
    select month_start, subscription_id from status
    union select month_start, subscription_id from usage   where month_start <= (select max(month_start) from status)
    union select month_start, subscription_id from revenue where month_start <= (select max(month_start) from status)
)
select
    sp.month_start,
    sp.subscription_id,
    p.account_id,
    p.plan_id,
    p.contract_type,
    p.home_region_name                                      as region_name,
    p.brand_name,
    p.is_reportable,
    st.status_bom,
    st.status_eom,
    coalesce(st.is_in_base_bom, false)                      as is_in_base_bom,
    coalesce(st.is_in_base_eom, false)                      as is_in_base_eom,
    coalesce(st.is_gross_add, false)                        as is_gross_add,
    coalesce(st.is_churned, false)                          as is_churned,
    zeroifnull(u.active_days)                               as active_days,
    zeroifnull(u.voice_calls)                               as voice_calls,
    round(zeroifnull(u.voice_min), 2)                       as voice_min,
    zeroifnull(u.sms_count)                                 as sms_count,
    round(zeroifnull(u.data_mb), 3)                         as data_mb,
    round(zeroifnull(u.data_mb_5g), 3)                      as data_mb_5g,
    zeroifnull(r.plan_revenue_php)                          as plan_revenue_php,
    zeroifnull(r.recharge_revenue_php)                      as recharge_revenue_php,
    zeroifnull(r.vas_revenue_php)                           as vas_revenue_php,
    zeroifnull(r.roaming_revenue_php)                       as roaming_revenue_php,
    zeroifnull(r.total_revenue_php)                         as total_revenue_php,
    zeroifnull(rc.recharges)                                as recharges,
    zeroifnull(rc.recharge_amount_php)                      as recharge_amount_php,
    zeroifnull(t.tickets)                                   as tickets,
    zeroifnull(u.active_days) > 0                           as is_usage_active
from spine sp
left join status    st on st.subscription_id = sp.subscription_id and st.month_start = sp.month_start
left join usage     u  on u.subscription_id  = sp.subscription_id and u.month_start  = sp.month_start
left join revenue   r  on r.subscription_id  = sp.subscription_id and r.month_start  = sp.month_start
left join recharges rc on rc.subscription_id = sp.subscription_id and rc.month_start = sp.month_start
left join tickets   t  on t.subscription_id  = sp.subscription_id and t.month_start  = sp.month_start
left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = sp.subscription_id
