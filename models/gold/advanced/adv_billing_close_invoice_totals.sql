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
  adv_billing_close_invoice_totals  -  Billing close, step 1 of 3: invoice totals
  ------------------------------------------------------------------------------------------
  Ported from x_h15_invoice_totals (root task of the PIPE_X H15 task graph ->
  PIPE_X.SP_X_H15_STEP1_INVOICES). dbt ref() ordering replaces the task graph.
  Grain : one row per close_month x account_id (every billing month in the loaded window).
  Change vs. original: ephemeral -> transient table (so it can be tested), all close months
  instead of one var(close_month), silver invoice/lines (deduplicated invoice ids).
  Orphan invoices (NULL account) get their account from SOURCE_CUSTOMER_REF where possible
  (macro f4_close_invoices); unattributable ones are excluded.
  Snowflake-specific: IFF, ZEROIFNULL, COUNT_IF, ARRAY_AGG ... WITHIN GROUP.
  Refs (4): slv_invoice, slv_invoice_line, slv_customer + slv_account (orphan recovery).
-#}
WITH lines AS (
    SELECT
        l.invoice_id,
        SUM(IFF(l.line_type = 'PLAN_FEE', l.line_amount_php, 0))    AS plan_fee_php,
        SUM(IFF(l.line_type = 'ADDON',    l.line_amount_php, 0))    AS addon_php,
        SUM(IFF(l.line_type = 'ROAMING',  l.line_amount_php, 0))    AS roaming_php,
        SUM(IFF(l.line_type = 'VAS',      l.line_amount_php, 0))    AS vas_php,
        SUM(IFF(l.line_type = 'DISCOUNT', l.line_amount_php, 0))    AS discount_php,
        SUM(IFF(l.line_type = 'TAX',      l.line_amount_php, 0))    AS tax_php,
        SUM(l.line_amount_php)                                      AS lines_php,
        SUM(l.line_amount_precise_php)                              AS lines_precise_php,
        COUNT(*)                                                    AS line_count
    FROM {{ ref('slv_invoice_line') }} l
    GROUP BY l.invoice_id
)
SELECT
    i.billing_month                                                 AS close_month,
    i.account_id,
    COUNT(*)                                                        AS invoices,
    SUM(i.invoice_amount_php)                                       AS billed_php,
    SUM(ZEROIFNULL(l.lines_php))                                    AS lines_php,
    SUM(ZEROIFNULL(l.lines_precise_php))                            AS lines_precise_php,
    SUM(ZEROIFNULL(l.plan_fee_php))                                 AS plan_fee_php,
    SUM(ZEROIFNULL(l.addon_php))                                    AS addon_php,
    SUM(ZEROIFNULL(l.roaming_php))                                  AS roaming_php,
    SUM(ZEROIFNULL(l.vas_php))                                      AS vas_php,
    SUM(ZEROIFNULL(l.discount_php))                                 AS discount_php,
    SUM(ZEROIFNULL(l.tax_php))                                      AS tax_php,
    SUM(ZEROIFNULL(l.line_count))                                   AS line_count,
    MIN(i.issue_date)                                               AS first_issue_date,
    MAX(i.due_date)                                                 AS last_due_date,
    COUNT_IF(i.invoice_status = 'OVERDUE')                          AS invoices_flagged_overdue,
    ARRAY_AGG(i.invoice_id) WITHIN GROUP (ORDER BY i.invoice_id)    AS invoice_ids,
    COUNT(DISTINCT i.currency_code)                                 AS currencies,
    COUNT_IF(i.account_resolution = 'FROM_CUSTOMER_REF')            AS invoices_account_recovered
FROM ({{ f4_close_invoices() }}) i
LEFT JOIN lines l
       ON l.invoice_id = i.invoice_id
WHERE i.billing_month >= DATE_TRUNC('month', '{{ var("history_start_date") }}'::DATE)
GROUP BY i.billing_month, i.account_id
