{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['close_month', 'account_id'],
    merge_exclude_columns = ['first_loaded_at'],
    on_schema_change = 'append_new_columns',
    cluster_by = ['close_month'],
    tags = ['gold', 'advanced', 'h15', 'billing_close', 'finance'],
    query_tag = 'dbt_telco_h15'
  )
}}
{#-
  adv_billing_close  -  Billing close, step 3 of 3: account reconciliation
  ------------------------------------------------------------------------------------------
  Ported from x_h15_task_graph_billing_close (PIPE_X.SP_X_H15_TASK_GRAPH_BILLING_CLOSE +
  SP_X_H15_STEP3_RECON; was root task -> child -> child + control table).
  Grain : one row per close_month x account_id (test customers excluded).
  Reconciles billed vs invoice lines, payments (on time / late), adjustments by reason
  (PIVOT), late fees charged vs recomputed from the rule table, dunning, open balance,
  six-month payment behaviour and the account's subscribers.

  Changes vs. original:
    * every close month in the window (merge on close_month+account, latest 2 months
      re-merged on incremental runs) instead of one var(close_month);
    * BUG FIX: bronze ADJUSTMENT.AMOUNT is negative; the original subtracted it in the
      balance (i.e. ADDED credits to the balance). Credits now use the positive
      credit_amount_php from silver; pivoted reason columns are positive credits too;
    * timestamps from silver are already Manila-local; test customers flagged in silver.

  Snowflake-specific: ref() DAG replacing a TASK graph, PIVOT, range lookup with BETWEEN,
  QUALIFY, MAX_BY, MODE, EQUAL_NULL, DECODE(TRUE, ...), ARRAY_SIZE, DIV0, ZEROIFNULL,
  OBJECT_CONSTRUCT_KEEP_NULL, merge_exclude_columns.
  Refs/sources (14): adv_billing_close_invoice_totals, adv_billing_close_payment_totals,
  slv_invoice, slv_adjustment, slv_late_fee_assessment, source billing.late_fee_rule,
  slv_dunning_event, slv_payment, slv_account, slv_customer, slv_subscription, slv_plan,
  fct_revenue_daily.
-#}

WITH params AS (
    SELECT
        close_month,
        LAST_DAY(close_month)                                       AS close_month_end,
        DATEADD('day', 45, close_month)                             AS cutoff_date,
        DATEADD('month', -6, close_month)                           AS history_start
    FROM (SELECT DISTINCT close_month FROM {{ ref('adv_billing_close_invoice_totals') }})
    {%- if is_incremental() %}
    WHERE close_month >= (SELECT COALESCE(DATEADD('month', -1, MAX(close_month)), '1900-01-01'::DATE) FROM {{ this }})
    {%- endif %}
),

invoice_totals AS (
    SELECT it.*
    FROM {{ ref('adv_billing_close_invoice_totals') }} it
    JOIN params p ON p.close_month = it.close_month
),

payment_totals AS (
    SELECT pt.*
    FROM {{ ref('adv_billing_close_payment_totals') }} pt
    JOIN params p ON p.close_month = pt.close_month
),

close_invoices AS (
    SELECT i.invoice_id, i.account_id, i.billing_month AS close_month, i.invoice_amount_php, i.due_date,
           p.cutoff_date, p.close_month_end
    FROM ({{ f4_close_invoices() }}) i
    JOIN params p ON p.close_month = i.billing_month
),

/* ---------- 1. adjustments on the close month's invoices, by reason (PIVOT) ---------- */
adjustments_raw AS (
    SELECT
        ci.close_month,
        ci.account_id,
        a.reason_code,
        a.credit_amount_php                                         AS amount
    FROM close_invoices ci
    JOIN {{ ref('slv_adjustment') }} a
      ON a.invoice_id = ci.invoice_id
    WHERE a.adjustment_date <= ci.cutoff_date
),

adjustments AS (
    SELECT *
    FROM adjustments_raw
    PIVOT (SUM(amount) FOR reason_code IN ('OUTAGE_CREDIT', 'GOODWILL', 'BILLING_ERROR', 'REFUND'))
        AS pv (close_month, account_id, adj_outage_credit, adj_goodwill, adj_billing_error, adj_refund)
),

adjustment_counts AS (
    SELECT
        close_month,
        account_id,
        COUNT(*)                                                    AS adjustments,
        SUM(amount)                                                 AS adjusted_php
    FROM adjustments_raw
    GROUP BY close_month, account_id
),

/* ---------- 2. late fees: charged vs recomputed from the rule table ---------- */
late_fees AS (
    SELECT
        ci.close_month,
        ci.account_id,
        f.invoice_id,
        f.rule_code,
        f.days_overdue,
        f.fee_php,
        ci.invoice_amount_php,
        r.rule_code                                                 AS expected_rule_code,
        GREATEST(ROUND(ci.invoice_amount_php * r.rate, 2), r.min_fee_php) AS expected_fee_php
    FROM close_invoices ci
    JOIN {{ ref('slv_late_fee_assessment') }} f
      ON f.invoice_id = ci.invoice_id
    LEFT JOIN {{ source('billing', 'late_fee_rule') }} r
           ON f.days_overdue BETWEEN r.min_days AND COALESCE(r.max_days, 99999)
    WHERE f.assessment_date <= ci.cutoff_date
    QUALIFY ROW_NUMBER() OVER (PARTITION BY f.invoice_id, f.assessment_date ORDER BY r.min_days DESC NULLS LAST) = 1
),

late_fee_totals AS (
    SELECT
        close_month,
        account_id,
        COUNT(*)                                                    AS late_fee_count,
        SUM(fee_php)                                                AS late_fees_php,
        SUM(ZEROIFNULL(expected_fee_php))                           AS expected_late_fees_php,
        COUNT_IF(NOT EQUAL_NULL(rule_code, expected_rule_code))     AS wrong_rule_count,
        MAX(days_overdue)                                           AS max_days_overdue,
        MAX_BY(rule_code, days_overdue)                             AS worst_rule_code
    FROM late_fees
    GROUP BY close_month, account_id
),

/* ---------- 3. dunning on the close month's invoices ---------- */
dunning AS (
    SELECT
        ci.close_month,
        ci.account_id,
        COUNT(*)                                                    AS dunning_events,
        MAX(d.dunning_stage)                                        AS max_dunning_stage,
        COUNT_IF(d.dunning_action = 'SMS_REMINDER')                 AS sms_reminders,
        COUNT_IF(d.dunning_action = 'EMAIL_NOTICE')                 AS email_notices,
        COUNT_IF(d.dunning_action = 'SERVICE_BARRED')               AS barrings,
        MAX_BY(d.dunning_action, d.action_at_utc)                   AS last_dunning_action,
        MAX(d.action_at_utc)                                        AS last_dunning_at_utc
    FROM close_invoices ci
    JOIN {{ ref('slv_dunning_event') }} d
      ON d.invoice_id = ci.invoice_id
    WHERE d.action_date <= ci.cutoff_date
    GROUP BY ci.close_month, ci.account_id
),

/* ---------- 4. six-month payment behaviour of the account ---------- */
history AS (
    SELECT
        p.close_month,
        i.account_id,
        COUNT(DISTINCT i.invoice_id)                                AS invoices_6m,
        COUNT(DISTINCT IFF(p2.payment_date <= i.due_date, i.invoice_id, NULL)) AS on_time_6m,
        COUNT(DISTINCT IFF(p2.payment_id IS NULL, i.invoice_id, NULL)) AS never_paid_6m,
        SUM(i.invoice_amount_php)                                   AS billed_6m_php
    FROM params p
    JOIN {{ ref('slv_invoice') }} i
      ON i.billing_month >= p.history_start
     AND i.billing_month <  p.close_month
    LEFT JOIN {{ ref('slv_payment') }} p2
           ON p2.invoice_id = i.invoice_id
    GROUP BY p.close_month, i.account_id
),

/* ---------- 5. account, customer and subscribers ---------- */
account_info AS (
    SELECT
        a.account_id,
        a.payment_method                                            AS account_payment_method,
        a.is_paperless_billing,
        a.billing_cycle_day,
        a.created_at_utc,
        c.customer_code,
        c.is_senior,
        c.is_test_customer
    FROM {{ ref('slv_account') }} a
    LEFT JOIN {{ ref('slv_customer') }} c
           ON c.customer_id = a.customer_id
),

subscribers AS (
    SELECT
        s.account_id,
        COUNT(*)                                                    AS subscriptions,
        COUNT_IF(s.status = 'ACTIVE')                               AS active_subscriptions,
        COUNT_IF(s.status = 'SUSPENDED')                            AS suspended_subscriptions,
        SUM(s.monthly_charges_php)                                  AS monthly_charges,
        MODE(pc.contract_type)                                      AS main_contract_type
    FROM {{ ref('slv_subscription') }} s
    LEFT JOIN {{ ref('slv_plan') }} pc
           ON pc.plan_id = s.plan_id
    WHERE NOT s.is_deleted
    GROUP BY s.account_id
),

earned AS (
    SELECT
        p.close_month,
        s.account_id,
        SUM(r.total_revenue_php)                                    AS earned_php
    FROM params p
    JOIN {{ ref('fct_revenue_daily') }} r
      ON r.business_date >= p.close_month
     AND r.business_date <= p.close_month_end
    JOIN {{ ref('slv_subscription') }} s
      ON s.subscription_id = r.subscription_id
    GROUP BY p.close_month, s.account_id
),

/* ---------- 6. reconcile ---------- */
reconciled AS (
    SELECT
        it.close_month,
        it.account_id,
        it.invoices,
        it.line_count,
        ARRAY_SIZE(it.invoice_ids)                                  AS invoice_id_count,
        it.first_issue_date,
        it.last_due_date,
        it.billed_php,
        it.lines_php,
        it.lines_precise_php,
        it.plan_fee_php,
        it.addon_php,
        it.roaming_php,
        it.vas_php,
        it.discount_php,
        it.tax_php,
        it.invoices_flagged_overdue,
        ZEROIFNULL(pt.payments)                                     AS payments,
        ZEROIFNULL(pt.paid_php)                                     AS paid_php,
        ZEROIFNULL(pt.paid_on_time_php)                             AS paid_on_time_php,
        ZEROIFNULL(pt.paid_late_php)                                AS paid_late_php,
        pt.first_paid_at_utc,
        pt.last_paid_at_utc,
        pt.max_days_late,
        pt.main_payment_method,
        ZEROIFNULL(ac.adjustments)                                  AS adjustments,
        ZEROIFNULL(ac.adjusted_php)                                 AS adjusted_php,
        ZEROIFNULL(ad.adj_outage_credit)                            AS adj_outage_credit_php,
        ZEROIFNULL(ad.adj_goodwill)                                 AS adj_goodwill_php,
        ZEROIFNULL(ad.adj_billing_error)                            AS adj_billing_error_php,
        ZEROIFNULL(ad.adj_refund)                                   AS adj_refund_php,
        ZEROIFNULL(lf.late_fee_count)                               AS late_fee_count,
        ZEROIFNULL(lf.late_fees_php)                                AS late_fees_php,
        ZEROIFNULL(lf.expected_late_fees_php)                       AS expected_late_fees_php,
        ZEROIFNULL(lf.wrong_rule_count)                             AS wrong_late_fee_rules,
        lf.max_days_overdue,
        lf.worst_rule_code,
        ZEROIFNULL(dn.dunning_events)                               AS dunning_events,
        ZEROIFNULL(dn.max_dunning_stage)                            AS max_dunning_stage,
        ZEROIFNULL(dn.sms_reminders)                                AS sms_reminders,
        ZEROIFNULL(dn.email_notices)                                AS email_notices,
        ZEROIFNULL(dn.barrings)                                     AS barrings,
        dn.last_dunning_action,
        ZEROIFNULL(hi.invoices_6m)                                  AS invoices_6m,
        ROUND(DIV0(hi.on_time_6m, hi.invoices_6m), 4)               AS on_time_rate_6m,
        ZEROIFNULL(hi.never_paid_6m)                                AS never_paid_6m,
        ai.account_payment_method,
        ai.is_paperless_billing,
        ai.billing_cycle_day,
        DATEDIFF('month', ai.created_at_utc, p.close_month_end)     AS account_age_months,
        ai.customer_code,
        ai.is_senior,
        COALESCE(ai.is_test_customer, FALSE)                        AS is_test_customer,
        ZEROIFNULL(sb.subscriptions)                                AS subscriptions,
        ZEROIFNULL(sb.active_subscriptions)                         AS active_subscriptions,
        ZEROIFNULL(sb.suspended_subscriptions)                      AS suspended_subscriptions,
        sb.monthly_charges,
        sb.main_contract_type,
        ZEROIFNULL(ea.earned_php)                                   AS earned_php
    FROM invoice_totals it
    JOIN params p                   ON p.close_month = it.close_month
    LEFT JOIN payment_totals pt     ON pt.close_month = it.close_month AND pt.account_id = it.account_id
    LEFT JOIN adjustments ad        ON ad.close_month = it.close_month AND ad.account_id = it.account_id
    LEFT JOIN adjustment_counts ac  ON ac.close_month = it.close_month AND ac.account_id = it.account_id
    LEFT JOIN late_fee_totals lf    ON lf.close_month = it.close_month AND lf.account_id = it.account_id
    LEFT JOIN dunning dn            ON dn.close_month = it.close_month AND dn.account_id = it.account_id
    LEFT JOIN history hi            ON hi.close_month = it.close_month AND hi.account_id = it.account_id
    LEFT JOIN account_info ai       ON ai.account_id = it.account_id
    LEFT JOIN subscribers sb        ON sb.account_id = it.account_id
    LEFT JOIN earned ea             ON ea.close_month = it.close_month AND ea.account_id = it.account_id
),

balanced AS (
    SELECT
        r.*,
        r.billed_php + r.late_fees_php - r.paid_php - r.adjusted_php AS balance_php,
        ROUND(r.billed_php - r.lines_php, 2)                        AS billed_vs_lines_diff,
        ROUND(r.lines_precise_php - r.lines_php, 6)                 AS rounding_diff,
        ROUND(r.late_fees_php - r.expected_late_fees_php, 2)        AS late_fee_diff,
        ROUND(r.billed_php - r.earned_php, 2)                       AS billed_vs_earned_diff
    FROM reconciled r
)

SELECT
    b.close_month,
    b.account_id,
    b.customer_code,
    b.invoices,
    b.line_count,
    b.first_issue_date,
    b.last_due_date,
    b.billed_php,
    b.lines_php,
    b.plan_fee_php,
    b.addon_php,
    b.roaming_php,
    b.vas_php,
    b.discount_php,
    b.tax_php,
    b.payments,
    b.paid_php,
    b.paid_on_time_php,
    b.paid_late_php,
    b.first_paid_at_utc,
    b.last_paid_at_utc,
    b.max_days_late,
    b.main_payment_method,
    b.adjustments,
    b.adjusted_php,
    b.adj_outage_credit_php,
    b.adj_goodwill_php,
    b.adj_billing_error_php,
    b.adj_refund_php,
    b.late_fee_count,
    b.late_fees_php,
    b.expected_late_fees_php,
    b.late_fee_diff,
    b.wrong_late_fee_rules,
    b.max_days_overdue,
    b.worst_rule_code,
    ROUND(b.balance_php, 2)                                         AS balance_php,
    CASE
        WHEN b.balance_php <= 0.01 THEN 'SETTLED'
        WHEN b.paid_php > 0        THEN 'PARTIAL'
        ELSE 'UNPAID'
    END                                                             AS close_status,
    CASE
        WHEN b.balance_php <= 0.01                                  THEN 'NONE'
        WHEN b.max_days_overdue IS NULL OR b.max_days_overdue <= 30 THEN '0-30'
        WHEN b.max_days_overdue <= 60                               THEN '31-60'
        WHEN b.max_days_overdue <= 90                               THEN '61-90'
        ELSE '90+'
    END                                                             AS aging_bucket,
    IFF(b.balance_php < -0.01, -b.balance_php, 0)                   AS credit_balance_php,
    ROUND(DIV0(b.paid_php, b.billed_php + b.late_fees_php), 4)      AS collection_rate,
    b.billed_vs_lines_diff,
    b.rounding_diff,
    b.billed_vs_earned_diff,
    b.dunning_events,
    b.max_dunning_stage,
    b.sms_reminders,
    b.email_notices,
    b.barrings,
    b.last_dunning_action,
    IFF(b.barrings > 0 AND b.balance_php <= 0.01, TRUE, FALSE)      AS barred_but_settled,
    b.invoices_6m,
    b.on_time_rate_6m,
    b.never_paid_6m,
    DECODE(TRUE,
           b.never_paid_6m >= 2, 'HIGH',
           b.on_time_rate_6m < 0.5, 'MEDIUM',
           'LOW')                                                   AS credit_risk,
    b.account_payment_method,
    IFF(NOT EQUAL_NULL(b.account_payment_method, b.main_payment_method) AND b.payments > 0, TRUE, FALSE) AS paid_with_other_method,
    b.is_paperless_billing,
    b.billing_cycle_day,
    b.account_age_months,
    b.is_senior,
    b.subscriptions,
    b.active_subscriptions,
    b.suspended_subscriptions,
    b.monthly_charges,
    b.main_contract_type,
    b.earned_php,
    OBJECT_CONSTRUCT_KEEP_NULL(
        'billed', b.billed_php,
        'paid', b.paid_php,
        'adjusted', b.adjusted_php,
        'late_fees', b.late_fees_php,
        'balance', ROUND(b.balance_php, 2)
    )                                                               AS close_summary,
    RANK() OVER (PARTITION BY b.close_month ORDER BY b.balance_php DESC, b.account_id) AS balance_rank,
    CURRENT_TIMESTAMP()                                             AS first_loaded_at,
    CURRENT_TIMESTAMP()                                             AS loaded_at
FROM balanced b
WHERE NOT b.is_test_customer
