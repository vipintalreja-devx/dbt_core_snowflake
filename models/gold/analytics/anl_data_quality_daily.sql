{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='check_date',
    on_schema_change='append_new_columns',
    cluster_by=['check_date'],
    tags=['gold', 'analytics', 'data_quality', 'm33']
) }}
-- Daily data-quality scorecard over the BRONZE feeds (what arrived) and SILVER (what survived):
-- duplicates removed, NaN counters, parse failures, negative payments, invoice integrity.
-- Ported from x_m33_js_data_quality (JavaScript procedure running 6 checks in a loop) - now one
-- set-based model producing one row per check per day for the whole window.
-- Grain: check_date x check_name.
-- Sources: source usage.cdr_voice, source usage.xdr_mediation_raw, source network.cell_performance_15min,
--          source billing.payment, slv_cdr_voice, slv_invoice, slv_invoice_line, slv_subscription.
with bronze_voice as (
    select convert_timezone('{{ var("local_tz") }}', call_start)::date as check_date,
           count(*) as rows_in, count(distinct call_id) as distinct_ids,
           count_if(duration_sec < 0) as negative_durations, count_if(subscription_id is null) as null_subscriber
    from {{ source('usage', 'cdr_voice') }}
    where call_start >= {{ incremental_ts_lower_bound('check_date') }}::timestamp_tz
    group by 1
),
silver_voice as (
    select event_date as check_date, count(*) as rows_out
    from {{ ref('slv_cdr_voice') }}
    where {{ incremental_date_filter('event_date', 'check_date') }}
    group by 1
),
xdr as (
    select convert_timezone('{{ var("local_tz") }}', received_at)::date as check_date,
           count(*) as records, count_if(parse_status = 'FAILED') as failed, count_if(parse_status = 'PARTIAL') as partial
    from {{ source('usage', 'xdr_mediation_raw') }}
    where received_at >= {{ incremental_ts_lower_bound('check_date') }}::timestamp_ltz
    group by 1
),
pm as (
    select {{ utc_to_local('interval_start') }}::date      as check_date,
           count(*) as intervals, count_if(equal_null(latency_ms, 'NaN'::float)) as nan_latency
    from {{ source('network', 'cell_performance_15min') }}
    where interval_start >= {{ incremental_ts_lower_bound('check_date') }}
    group by 1
),
pay as (
    select convert_timezone('{{ var("local_tz") }}', paid_at)::date as check_date,
           count(*) as payments, count_if(amount < 0) as negative_payments, count_if(invoice_id is null) as unallocated
    from {{ source('billing', 'payment') }}
    where paid_at >= {{ incremental_ts_lower_bound('check_date') }}::timestamp_ltz
    group by 1
),
inv as (
    select i.issue_date as check_date,
           count(*) as invoices,
           count_if(i.due_date < i.issue_date) as due_before_issue,
           count_if(abs(i.invoice_amount_php - zeroifnull(l.lines_php)) > 0.01) as header_line_mismatch,
           count_if(l.invoice_id is null) as invoices_without_lines
    from {{ ref('slv_invoice') }} i
    left join (select invoice_id, sum(line_amount_php) as lines_php from {{ ref('slv_invoice_line') }} group by 1) l
           on l.invoice_id = i.invoice_id
    where {{ incremental_date_filter('i.issue_date', 'check_date') }}
    group by 1
),
subs as (
    select current_date() as snapshot_date,
           count_if(plan_id is null and not is_deleted) as active_without_plan,
           count_if(msisdn is null or not rlike(msisdn, '^639[0-9]{9}$')) as bad_msisdn
    from {{ ref('slv_subscription') }}
),
checks as (
    select check_date, 'VOICE_DUPLICATE_CALL_ID' as check_name, 'usage' as domain, rows_in - distinct_ids as failed_rows, rows_in as checked_rows from bronze_voice
    union all select check_date, 'VOICE_NEGATIVE_DURATION', 'usage', negative_durations, rows_in from bronze_voice
    union all select check_date, 'VOICE_NULL_SUBSCRIBER', 'usage', null_subscriber, rows_in from bronze_voice
    union all select b.check_date, 'VOICE_BRONZE_TO_SILVER_LOSS', 'usage', b.distinct_ids - zeroifnull(s.rows_out), b.distinct_ids
              from bronze_voice b left join silver_voice s on s.check_date = b.check_date
    union all select check_date, 'XDR_PARSE_FAILED', 'mediation', failed, records from xdr
    union all select check_date, 'XDR_PARSE_PARTIAL', 'mediation', partial, records from xdr
    union all select check_date, 'PM_NAN_LATENCY', 'network', nan_latency, intervals from pm
    union all select check_date, 'PAYMENT_NEGATIVE_AMOUNT', 'billing', negative_payments, payments from pay
    union all select check_date, 'PAYMENT_UNALLOCATED', 'billing', unallocated, payments from pay
    union all select check_date, 'INVOICE_DUE_BEFORE_ISSUE', 'billing', due_before_issue, invoices from inv
    union all select check_date, 'INVOICE_HEADER_LINE_MISMATCH', 'billing', header_line_mismatch, invoices from inv
    union all select check_date, 'INVOICE_WITHOUT_LINES', 'billing', invoices_without_lines, invoices from inv
)
select
    c.check_date,
    c.check_name,
    c.domain,
    c.checked_rows,
    c.failed_rows,
    round(div0(c.failed_rows, c.checked_rows) * 100, 4)     as failed_pct,
    case when c.failed_rows = 0 then 'PASS'
         when div0(c.failed_rows, c.checked_rows) < 0.001 then 'WARN'
         else 'FAIL' end                                    as status,
    (select active_without_plan from subs)                  as active_lines_without_plan_today,
    (select bad_msisdn from subs)                           as bad_msisdn_today,
    current_timestamp()                                     as loaded_at
from checks c
where c.check_date >= '{{ var("history_start_date") }}'::date
  and c.check_date <= current_date()
