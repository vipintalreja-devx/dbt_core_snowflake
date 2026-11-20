{#- ===========================================================================
  F4 helpers for the ported advanced models (H13-H21).
  The original v2 models ran for ONE parameter (run_month / run_date / close_month).
  These CTE bodies expand that into every period of the loaded history window, so a
  model can compute all periods in one set-based statement and run incrementally
  (incremental runs re-process the latest loaded period plus one before it).
=========================================================================== -#}

{#- Monthly parameter spine: one row per reporting month. -#}
{% macro f4_months_cte(target_col='month_start') -%}
    select
        month_start                                         as run_month,
        last_day(month_start)                               as month_end,
        dateadd('month', 1, month_start)                    as next_month,
        dateadd('month', -1, month_start)                   as prev_month,
        dateadd('month', -11, month_start)                  as year_start
    from (select distinct month_start from {{ ref('int_subscriber_month_status') }}) ms
    {%- if is_incremental() %}
    where month_start >= (select coalesce(dateadd('month', -1, max({{ target_col }})), '1900-01-01'::date) from {{ this }})
    {%- endif %}
{%- endmacro %}

{#- Daily parameter spine between history_start_date and the last loaded usage day. -#}
{% macro f4_days_cte(target_col='event_date', max_date_sql=none) -%}
    select
        d.date_day                                          as run_date,
        dateadd('day', 1, d.date_day)                       as next_date,
        date_trunc('month', d.date_day)                     as run_month
    from {{ ref('dim_date') }} d
    where d.date_day >= '{{ var("history_start_date") }}'::date
      and d.date_day <= {{ max_date_sql or "(select max(event_date) from " ~ ref('fct_subscriber_usage_daily') ~ ")" }}
    {%- if is_incremental() %}
      and d.date_day >= (select coalesce(dateadd('day', -{{ var('lookback_days') }}, max({{ target_col }})), '1900-01-01'::date) from {{ this }})
    {%- endif %}
{%- endmacro %}

{#- Manila-midnight of a DATE as TIMESTAMP_TZ (replaces v2 manila_ts()). -#}
{% macro f4_manila_ts(date_expr) -%}
    to_timestamp_tz(to_varchar({{ date_expr }}) || ' 00:00:00 +08:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM')
{%- endmacro %}

{#- Currency conversion to PHP (replaces v2 to_php() / BILLING.FN_TO_PHP). -#}
{% macro f4_to_php(amount, currency, rate) -%}
    iff({{ currency }} = 'PHP', {{ amount }}::number(18, 4), ({{ amount }}::number(18, 4) * {{ rate }})::number(18, 4))
{%- endmacro %}

{#- Billing-close invoice set (H15 steps 1-3). Bronze has ~2.9k invoices with NULL
    ACCOUNT_ID; about half carry SOURCE_CUSTOMER_REF = a (case/space-mangled) customer_code,
    and accounts are 1:1 with customers, so the account is recovered from it. Invoices that
    still have no account cannot be attributed and are excluded from the close
    (account_resolution tells how each row got its account). -#}
{% macro f4_close_invoices() -%}
    select
        i.invoice_id,
        coalesce(i.account_id, ra.account_id)                   as account_id,
        iff(i.account_id is not null, 'DIRECT', 'FROM_CUSTOMER_REF') as account_resolution,
        i.billing_month,
        i.issue_date,
        i.due_date,
        i.invoice_amount_php,
        i.currency_code,
        i.invoice_status
    from {{ ref('slv_invoice') }} i
    left join {{ ref('slv_customer') }} rc
           on i.account_id is null
          and rc.customer_code = upper(trim(i.source_customer_ref))
    left join {{ ref('slv_account') }} ra
           on ra.customer_id = rc.customer_id
    where coalesce(i.account_id, ra.account_id) is not null
{%- endmacro %}
