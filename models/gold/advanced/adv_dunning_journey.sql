{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'dunning_month',
    on_schema_change = 'append_new_columns',
    transient = false,
    cluster_by = ['dunning_month', 'outcome'],
    tags = ['gold', 'advanced', 'h20', 'collections', 'journey'],
    query_tag = 'dbt_telco_h20'
  )
}}
{#-
  adv_dunning_journey  -  Collections (dunning) journey per invoice
  ------------------------------------------------------------------------------------------
  Ported from x_h20_dunning_journey_match (PIPE_X.SP_X_H20_DUNNING_JOURNEY_MATCH).
  Grain : one row per invoice journey (invoice_id x match_no), bucketed by dunning_month =
  Manila month of the invoice's first stage-1 reminder.
  All collection events of the invoice (reminder stages, payments, credits, late fees)
  are put in time order and matched with MATCH_RECOGNIZE; each journey gets its outcome,
  time between stages, money recovered, reminder channels and the account's history.

  Changes vs. original:
    * every month in the loaded window (incremental by dunning_month) instead of one run_month;
    * BUG FIX: bronze ADJUSTMENT.AMOUNT is negative, so "paid + adjusted >= total" and the
      remaining balance were wrong for every written-down invoice; credits are now the
      positive silver credit_amount_php;
    * event timeline in UTC (silver *_utc); date-only events (credits, late fees) are
      placed at Manila midnight of their date.

  Snowflake-specific: MATCH_RECOGNIZE (MEASURES with FIRST/LAST/COUNT/SUM, CLASSIFIER,
  MATCH_NUMBER, AFTER MATCH SKIP PAST LAST ROW), running SUM window, CONVERT_TIMEZONE,
  ARRAY_AGG DISTINCT, LISTAGG DISTINCT, MAX_BY, MODE, DIV0, ZEROIFNULL, IFF, DECODE, HASH.
  Refs (11): slv_dunning_event, slv_invoice, slv_payment, slv_adjustment,
  slv_late_fee_assessment, slv_invoice_line, slv_account, slv_customer, slv_subscription,
  slv_plan, slv_ticket.
-#}

WITH params AS (
    SELECT
        run_month,
        next_month,
        DATEADD('month', 3, run_month)                              AS follow_until
    FROM ({{ f4_months_cte('dunning_month') }})
),

/* ---------- 1. invoices that entered dunning in each month ---------- */
dunning_invoices AS (
    SELECT
        p.run_month                                                 AS dunning_month,
        p.follow_until,
        di.invoice_id,
        di.first_reminder_at_utc
    FROM (
        SELECT invoice_id, MIN(action_at_utc) AS first_reminder_at_utc, MIN(action_date) AS first_reminder_date
        FROM {{ ref('slv_dunning_event') }}
        WHERE dunning_stage = 1
        GROUP BY invoice_id
    ) di
    JOIN params p
      ON di.first_reminder_date >= p.run_month
     AND di.first_reminder_date <  p.next_month
),

invoice AS (
    SELECT
        i.invoice_id,
        i.account_id,
        i.billing_month,
        i.issue_date,
        i.due_date,
        i.invoice_amount_php,
        i.invoice_status,
        di.dunning_month,
        di.follow_until,
        di.first_reminder_at_utc
    FROM {{ ref('slv_invoice') }} i
    JOIN dunning_invoices di
      ON di.invoice_id = i.invoice_id
),

/* ---------- 2. one ordered event stream per invoice ---------- */
events AS (
    SELECT d.invoice_id, d.action_at_utc AS ts, 'S' || d.dunning_stage AS ev, d.dunning_action AS detail, d.amount_due_php AS amount
    FROM {{ ref('slv_dunning_event') }} d
    JOIN invoice i ON i.invoice_id = d.invoice_id
    UNION ALL
    SELECT pm.invoice_id, pm.paid_at_utc, 'PAY', pm.payment_method, pm.payment_amount_php
    FROM {{ ref('slv_payment') }} pm
    JOIN invoice i ON i.invoice_id = pm.invoice_id
    WHERE pm.payment_date < i.follow_until
    UNION ALL
    SELECT a.invoice_id,
           CONVERT_TIMEZONE('{{ var("local_tz") }}', 'UTC', a.adjustment_date::TIMESTAMP_NTZ),
           'ADJ', a.reason_code, a.credit_amount_php
    FROM {{ ref('slv_adjustment') }} a
    JOIN invoice i ON i.invoice_id = a.invoice_id
    WHERE a.adjustment_date < i.follow_until
    UNION ALL
    SELECT f.invoice_id,
           CONVERT_TIMEZONE('{{ var("local_tz") }}', 'UTC', f.assessment_date::TIMESTAMP_NTZ),
           'FEE', f.rule_code, f.fee_php
    FROM {{ ref('slv_late_fee_assessment') }} f
    JOIN invoice i ON i.invoice_id = f.invoice_id
    WHERE f.assessment_date < i.follow_until
),

events_running AS (
    SELECT
        e.*,
        SUM(IFF(e.ev = 'PAY', e.amount, 0)) OVER (PARTITION BY e.invoice_id ORDER BY e.ts, e.ev
                                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS paid_so_far
    FROM events e
),

/* ---------- 3. match the journey ---------- */
journeys AS (
    SELECT *
    FROM events_running
    MATCH_RECOGNIZE (
        PARTITION BY invoice_id
        ORDER BY ts, ev
        MEASURES
            MATCH_NUMBER()                                          AS match_no,
            FIRST(s1.ts)                                            AS first_reminder_at_utc,
            FIRST(s2.ts)                                            AS stage2_at_utc,
            FIRST(s3.ts)                                            AS stage3_at_utc,
            LAST(ts)                                                AS last_event_at_utc,
            COUNT(s1.*)                                             AS stage1_events,
            COUNT(s2.*)                                             AS stage2_events,
            COUNT(s3.*)                                             AS stage3_events,
            COUNT(pay.*)                                            AS payments,
            FIRST(pay.ts)                                           AS first_pay_at_utc,
            LAST(pay.ts)                                            AS last_pay_at_utc,
            SUM(pay.amount)                                         AS paid_php,
            COUNT(adj.*)                                            AS adjustments,
            SUM(adj.amount)                                         AS adjusted_php,
            COUNT(fee.*)                                            AS late_fees,
            SUM(fee.amount)                                         AS late_fee_php,
            MAX(s1.amount)                                          AS amount_due_at_s1,
            MAX(s3.amount)                                          AS amount_due_at_s3,
            LAST(paid_so_far)                                       AS paid_total_at_end,
            CLASSIFIER()                                            AS last_step
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN ( s1+ (fee | adj)* s2* (fee | adj)* s3* (pay | adj | fee)* )
        DEFINE
            s1  AS ev = 'S1',
            s2  AS ev = 'S2',
            s3  AS ev = 'S3',
            pay AS ev = 'PAY',
            adj AS ev = 'ADJ',
            fee AS ev = 'FEE'
    )
),

channels AS (
    SELECT
        e.invoice_id,
        LISTAGG(DISTINCT e.detail, ',') WITHIN GROUP (ORDER BY e.detail) AS reminder_actions,
        COUNT_IF(e.detail = 'SMS_REMINDER')                         AS sms_reminders,
        COUNT_IF(e.detail = 'EMAIL_NOTICE')                         AS email_notices,
        MAX_BY(e.detail, e.ts)                                      AS last_reminder_action
    FROM events e
    WHERE e.ev IN ('S1', 'S2', 'S3')
    GROUP BY e.invoice_id
),

pay_methods AS (
    SELECT invoice_id, ARRAY_AGG(DISTINCT detail) AS payment_methods
    FROM events
    WHERE ev = 'PAY'
    GROUP BY invoice_id
),

/* ---------- 4. the account around the invoice ---------- */
account_info AS (
    SELECT
        a.account_id,
        a.payment_method                                            AS account_payment_method,
        a.is_paperless_billing,
        c.is_senior,
        c.preferred_language,
        COALESCE(c.is_test_customer, FALSE)                         AS is_test_customer
    FROM {{ ref('slv_account') }} a
    LEFT JOIN {{ ref('slv_customer') }} c
           ON c.customer_id = a.customer_id
),

account_subs AS (
    SELECT
        s.account_id,
        COUNT(*)                                                    AS subscriptions,
        COUNT_IF(s.status = 'SUSPENDED')                            AS suspended_subs,
        MODE(pc.contract_type)                                      AS main_contract_type
    FROM {{ ref('slv_subscription') }} s
    LEFT JOIN {{ ref('slv_plan') }} pc
           ON pc.plan_id = s.plan_id
    WHERE NOT s.is_deleted
    GROUP BY s.account_id
),

prior_dunning AS (
    SELECT
        i.invoice_id,
        COUNT(DISTINCT d.invoice_id)                                AS prior_dunned_invoices_12m,
        MAX(d.dunning_stage)                                        AS prior_max_stage_12m
    FROM invoice i
    JOIN {{ ref('slv_dunning_event') }} d
      ON d.account_id = i.account_id
     AND d.invoice_id <> i.invoice_id
     AND d.action_at_utc <  i.first_reminder_at_utc
     AND d.action_at_utc >= DATEADD('month', -12, i.first_reminder_at_utc)
    GROUP BY i.invoice_id
),

billing_tickets AS (
    SELECT
        i.invoice_id,
        COUNT(*)                                                    AS billing_tickets_during
    FROM invoice i
    JOIN {{ ref('slv_subscription') }} s
      ON s.account_id = i.account_id
    JOIN {{ ref('slv_ticket') }} t
      ON t.subscription_id = s.subscription_id
     AND t.category = 'BILLING'
     AND t.opened_at_utc >= DATEADD('day', -14, i.first_reminder_at_utc)
     AND t.opened_at_utc <  DATEADD('day', 60, i.first_reminder_at_utc)
    GROUP BY i.invoice_id
),

invoice_lines AS (
    SELECT
        l.invoice_id,
        SUM(IFF(l.line_type = 'ROAMING',  l.line_amount_php, 0))    AS roaming_on_invoice,
        SUM(IFF(l.line_type = 'DISCOUNT', l.line_amount_php, 0))    AS discount_on_invoice
    FROM {{ ref('slv_invoice_line') }} l
    JOIN invoice i ON i.invoice_id = l.invoice_id
    GROUP BY l.invoice_id
)

SELECT
    i.dunning_month,
    j.invoice_id,
    i.account_id,
    i.billing_month,
    i.due_date,
    i.invoice_amount_php                                            AS total_amount,
    i.invoice_status,
    j.match_no,
    j.first_reminder_at_utc,
    j.stage2_at_utc,
    j.stage3_at_utc,
    j.last_event_at_utc,
    j.stage1_events,
    j.stage2_events,
    j.stage3_events,
    j.payments,
    j.first_pay_at_utc,
    j.last_pay_at_utc,
    ZEROIFNULL(j.paid_php)                                          AS paid_php,
    j.adjustments,
    ZEROIFNULL(j.adjusted_php)                                      AS adjusted_php,
    j.late_fees,
    ZEROIFNULL(j.late_fee_php)                                      AS late_fee_php,
    j.amount_due_at_s1,
    j.amount_due_at_s3,
    ROUND(i.invoice_amount_php + ZEROIFNULL(j.late_fee_php) - ZEROIFNULL(j.paid_php) - ZEROIFNULL(j.adjusted_php), 2) AS remaining_php,
    ROUND(DIV0(j.paid_php, i.invoice_amount_php + ZEROIFNULL(j.late_fee_php)), 4) AS recovery_rate,
    j.last_step,
    CASE
        WHEN j.payments > 0 AND j.stage2_events = 0 AND j.stage3_events = 0
             AND ZEROIFNULL(j.paid_php) + ZEROIFNULL(j.adjusted_php) >= i.invoice_amount_php - 0.01 THEN 'CURED_AT_STAGE_1'
        WHEN j.payments > 0 AND j.stage3_events = 0
             AND ZEROIFNULL(j.paid_php) + ZEROIFNULL(j.adjusted_php) >= i.invoice_amount_php - 0.01 THEN 'CURED_AT_STAGE_2'
        WHEN j.payments > 0
             AND ZEROIFNULL(j.paid_php) + ZEROIFNULL(j.adjusted_php) >= i.invoice_amount_php - 0.01 THEN 'CURED_AFTER_BARRING'
        WHEN j.payments > 0                                         THEN 'PARTLY_PAID'
        WHEN j.adjustments > 0                                      THEN 'WRITTEN_DOWN'
        WHEN j.stage3_events > 0                                    THEN 'BARRED_UNPAID'
        ELSE 'IN_PROGRESS'
    END                                                             AS outcome,
    DATEDIFF('day', CONVERT_TIMEZONE('UTC', '{{ var("local_tz") }}', j.first_reminder_at_utc)::DATE,
                    CONVERT_TIMEZONE('UTC', '{{ var("local_tz") }}', j.first_pay_at_utc)::DATE) AS days_to_cure,
    DATEDIFF('day', j.first_reminder_at_utc, j.stage2_at_utc)       AS days_s1_to_s2,
    DATEDIFF('day', j.stage2_at_utc, j.stage3_at_utc)               AS days_s2_to_s3,
    DATEDIFF('day', i.due_date, CONVERT_TIMEZONE('UTC', '{{ var("local_tz") }}', j.first_reminder_at_utc)::DATE) AS days_due_to_reminder,
    DECODE(GREATEST(IFF(j.stage3_events > 0, 3, 0), IFF(j.stage2_events > 0, 2, 0), 1),
           1, 'STAGE_1', 2, 'STAGE_2', 3, 'STAGE_3')                AS deepest_stage,
    ch.reminder_actions,
    ch.sms_reminders,
    ch.email_notices,
    ch.last_reminder_action,
    pmx.payment_methods,
    ai.account_payment_method,
    ai.is_paperless_billing,
    ai.is_senior,
    ai.preferred_language,
    ZEROIFNULL(asb.subscriptions)                                   AS subscriptions,
    ZEROIFNULL(asb.suspended_subs)                                  AS suspended_subs,
    asb.main_contract_type,
    ZEROIFNULL(pd.prior_dunned_invoices_12m)                        AS prior_dunned_invoices_12m,
    pd.prior_max_stage_12m,
    IFF(pd.prior_dunned_invoices_12m >= 2, TRUE, FALSE)             AS repeat_defaulter,
    ZEROIFNULL(bt.billing_tickets_during)                           AS billing_tickets_during,
    IFF(bt.billing_tickets_during > 0, TRUE, FALSE)                 AS possible_dispute,
    ZEROIFNULL(il.roaming_on_invoice)                               AS roaming_on_invoice,
    ZEROIFNULL(il.discount_on_invoice)                              AS discount_on_invoice,
    HASH(j.invoice_id, j.match_no)                                  AS journey_key,
    CURRENT_TIMESTAMP()                                             AS loaded_at
FROM journeys j
JOIN invoice i               ON i.invoice_id = j.invoice_id
LEFT JOIN channels ch        ON ch.invoice_id = j.invoice_id
LEFT JOIN pay_methods pmx    ON pmx.invoice_id = j.invoice_id
LEFT JOIN account_info ai    ON ai.account_id = i.account_id
LEFT JOIN account_subs asb   ON asb.account_id = i.account_id
LEFT JOIN prior_dunning pd   ON pd.invoice_id = j.invoice_id
LEFT JOIN billing_tickets bt ON bt.invoice_id = j.invoice_id
LEFT JOIN invoice_lines il   ON il.invoice_id = j.invoice_id
WHERE NOT COALESCE(ai.is_test_customer, FALSE)
