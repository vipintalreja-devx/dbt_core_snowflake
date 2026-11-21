{{
  config(
    materialized = 'table',
    transient = true,
    cluster_by = ['close_month'],
    tags = ['gold', 'advanced', 'h15', 'billing_close', 'finance'],
    query_tag = 'dbt_telco_h15'
  )
}}
-- Materialization: table (current-state/small reference): billing-close step table, restated with the close DAG (adv_billing_close is the incremental output).
{#
  adv_billing_close_payment_totals  -  Billing close, step 2 of 3: payments
  ------------------------------------------------------------------------------------------
  Ported from x_h15_payment_totals (child task -> PIPE_X.SP_X_H15_STEP2_PAYMENTS).
  Payments against each close month's invoices received within 45 days of the month start.
  Grain : one row per close_month x account_id with at least one payment.
  Depends on step 1 (ref) so dbt enforces the task-graph order.
  Snowflake-specific: QUALIFY, MODE, IFF, DATEDIFF.
  Refs (5): adv_billing_close_invoice_totals (close-month list), slv_invoice (via
  f4_close_invoices: orphan accounts recovered), slv_customer, slv_account, slv_payment.
-#}
WITH close_months AS (
    SELECT DISTINCT close_month FROM {{ ref('adv_billing_close_invoice_totals') }}
),

pay AS (
    SELECT
        i.billing_month                                             AS close_month,
        i.account_id,
        i.invoice_id,
        i.due_date,
        p.payment_id,
        p.payment_amount_php,
        p.payment_method,
        p.paid_at_utc,
        p.payment_date                                              AS paid_date_mnl
    FROM ({{ f4_close_invoices() }}) i
    JOIN close_months cm
      ON cm.close_month = i.billing_month
    JOIN {{ ref('slv_payment') }} p
      ON p.invoice_id = i.invoice_id
    WHERE p.payment_date <= DATEADD('day', 45, i.billing_month)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.payment_id ORDER BY p.paid_at_utc) = 1
)
SELECT
    close_month,
    account_id,
    COUNT(payment_id)                                               AS payments,
    SUM(payment_amount_php)                                         AS paid_php,
    SUM(IFF(paid_date_mnl <= due_date, payment_amount_php, 0))      AS paid_on_time_php,
    SUM(IFF(paid_date_mnl >  due_date, payment_amount_php, 0))      AS paid_late_php,
    MIN(paid_at_utc)                                                AS first_paid_at_utc,
    MAX(paid_at_utc)                                                AS last_paid_at_utc,
    MAX(DATEDIFF('day', due_date, paid_date_mnl))                   AS max_days_late,
    MODE(payment_method)                                            AS main_payment_method,
    COUNT(DISTINCT payment_method)                                  AS payment_methods
FROM pay
GROUP BY close_month, account_id
