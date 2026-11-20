{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'doc_month',
    on_schema_change = 'append_new_columns',
    transient = false,
    copy_grants = true,
    cluster_by = ['doc_month'],
    tags = ['gold', 'advanced', 'h17', 'customer_360', 'json'],
    query_tag = 'dbt_telco_h17'
  )
}}
{#-
  adv_customer_360_json  -  Customer 360 JSON document per billed account and month
  ------------------------------------------------------------------------------------------
  Ported from x_h17_customer_360_json (PIPE_X.SP_X_H17_CUSTOMER_360_JSON).
  Grain : one row per doc_month x account_id (accounts invoiced in the month, no test customers).
  One VARIANT document per account: account, customer (KYC with document number and scan
  removed), consents, every subscription (plan, device, contract, partner brand, 30-day
  usage, revenue by stream, recharges, churn, segments, outages), the bill (lines as an
  object), the last six payments, tickets with the three latest, NPS. Flat columns next to
  the document make it searchable.

  Change vs. original: every billing month in the loaded window (incremental by month)
  instead of one run_month; silver/gold inputs (deduplicated invoices, Manila-local times,
  latest consent per type already resolved).

  Snowflake-specific: OBJECT_CONSTRUCT / OBJECT_CONSTRUCT_KEEP_NULL, OBJECT_AGG,
  ARRAY_AGG ... WITHIN GROUP, ARRAY_SLICE, ARRAY_SIZE, OBJECT_INSERT, OBJECT_DELETE,
  OBJECT_KEYS, TO_JSON, VARIANT paths, MAX_BY, QUALIFY, IFF, ZEROIFNULL, HASH, copy_grants.
  Refs/sources (19): slv_invoice, slv_invoice_line, slv_account, source core.customer (KYC),
  slv_customer, slv_customer_consent, slv_subscription, slv_plan, slv_device, slv_contract,
  slv_partner_brand, fct_subscriber_usage_daily, slv_daily_revenue, slv_payment, slv_ticket,
  slv_nps_survey, slv_churn_score, slv_segment_membership, slv_recharge, slv_outage_impact.
-#}

WITH params AS (
    SELECT
        run_month,
        month_end,
        next_month,
        DATEADD('day', -29, month_end)                              AS usage_start
    FROM ({{ f4_months_cte('doc_month') }})
),

/* ---------- 1. accounts billed this month and their bill ---------- */
invoices AS (
    SELECT
        p.run_month,
        i.invoice_id,
        i.account_id,
        i.invoice_amount_php,
        i.due_date,
        i.invoice_status
    FROM {{ ref('slv_invoice') }} i
    JOIN params p ON p.run_month = i.billing_month
),

bill AS (
    SELECT
        run_month,
        account_id,
        OBJECT_AGG(line_type, amt::VARIANT)                         AS bill_lines,
        SUM(amt)                                                    AS bill_total
    FROM (
        SELECT inv.run_month, inv.account_id, l.line_type, SUM(l.line_amount_php) AS amt
        FROM invoices inv
        JOIN {{ ref('slv_invoice_line') }} l
          ON l.invoice_id = inv.invoice_id
        GROUP BY inv.run_month, inv.account_id, l.line_type
    )
    GROUP BY run_month, account_id
),

invoice_summary AS (
    SELECT
        run_month,
        account_id,
        COUNT(*)                                                    AS invoices,
        SUM(invoice_amount_php)                                     AS invoiced_php,
        MAX(due_date)                                               AS due_date,
        MAX_BY(invoice_status, due_date)                            AS invoice_status
    FROM invoices
    GROUP BY run_month, account_id
),

billed_accounts AS (
    SELECT DISTINCT run_month, account_id FROM invoices
),

/* ---------- 2. customer and consents ---------- */
kyc AS (
    SELECT customer_id, OBJECT_DELETE(kyc_payload::OBJECT, 'doc_number', 'scan') AS kyc_safe
    FROM {{ source('core', 'customer') }}
),

customer AS (
    SELECT
        ba.run_month,
        a.account_id,
        a.payment_method,
        a.is_paperless_billing,
        a.billing_cycle_day,
        c.customer_id,
        c.customer_code,
        c.gender,
        c.is_senior,
        c.has_partner,
        c.has_dependents,
        c.preferred_language,
        DATEDIFF('year', c.date_of_birth, p.month_end)              AS age,
        k.kyc_safe,
        c.is_test_customer
    FROM billed_accounts ba
    JOIN params p                           ON p.run_month = ba.run_month
    JOIN {{ ref('slv_account') }} a         ON a.account_id = ba.account_id
    JOIN {{ ref('slv_customer') }} c        ON c.customer_id = a.customer_id
    LEFT JOIN kyc k                         ON k.customer_id = c.customer_id
),

consents_agg AS (
    SELECT
        customer_id,
        OBJECT_AGG(consent_type, OBJECT_CONSTRUCT('granted', is_granted, 'channels', channel_list)) AS consents
    FROM {{ ref('slv_customer_consent') }}
    GROUP BY customer_id
),

/* ---------- 3. subscriptions with everything about them ---------- */
billed_subs AS (
    SELECT ba.run_month, s.*
    FROM billed_accounts ba
    JOIN {{ ref('slv_subscription') }} s
      ON s.account_id = ba.account_id
    WHERE NOT s.is_deleted
),

sub_usage AS (
    SELECT
        p.run_month,
        u.subscription_id,
        SUM(u.data_mb)                                              AS data_mb_30d,
        SUM(u.voice_min)                                            AS voice_min_30d,
        SUM(u.sms_count)                                            AS sms_30d,
        COUNT(*)                                                    AS active_days_30d
    FROM params p
    JOIN {{ ref('fct_subscriber_usage_daily') }} u
      ON u.event_date >= p.usage_start
     AND u.event_date <= p.month_end
    GROUP BY p.run_month, u.subscription_id
),

sub_revenue AS (
    SELECT
        r.run_month,
        r.subscription_id,
        SUM(r.src_php)                                              AS revenue_php,
        OBJECT_AGG(r.revenue_source, r.src_php::VARIANT)            AS revenue_by_source
    FROM (
        SELECT p.run_month, dr.subscription_id, dr.revenue_source, SUM(dr.revenue_php) AS src_php
        FROM params p
        JOIN {{ ref('slv_daily_revenue') }} dr
          ON dr.business_date >= p.run_month
         AND dr.business_date <  p.next_month
        GROUP BY p.run_month, dr.subscription_id, dr.revenue_source
    ) r
    GROUP BY r.run_month, r.subscription_id
),

sub_contract AS (
    SELECT
        p.run_month,
        ct.subscription_id,
        OBJECT_CONSTRUCT('type', ct.contract_type, 'start', ct.start_date, 'end', ct.end_date,
                         'auto_renew', ct.is_auto_renew)            AS contract
    FROM params p
    JOIN {{ ref('slv_contract') }} ct
      ON ct.start_date <= p.month_end
     AND (ct.terminated_on IS NULL OR ct.terminated_on > p.month_end)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.run_month, ct.subscription_id ORDER BY ct.start_date DESC) = 1
),

sub_segments AS (
    SELECT
        p.run_month,
        sm.subscription_id,
        ARRAY_AGG(DISTINCT sm.segment_code)                         AS segments
    FROM params p
    JOIN {{ ref('slv_segment_membership') }} sm
      ON p.month_end >= sm.valid_from
     AND p.month_end <  sm.valid_to
    GROUP BY p.run_month, sm.subscription_id
),

sub_recharge AS (
    SELECT
        p.run_month,
        rc.subscription_id,
        COUNT(*)                                                    AS recharges,
        SUM(rc.recharge_amount_php)                                 AS recharge_php,
        MAX_BY(rc.pack_code, rc.recharged_at_local)                 AS last_pack
    FROM params p
    JOIN {{ ref('slv_recharge') }} rc
      ON rc.event_date >= p.run_month
     AND rc.event_date <  p.next_month
    GROUP BY p.run_month, rc.subscription_id
),

sub_outage AS (
    SELECT
        subscription_id,
        COUNT(*)                                                    AS outages,
        SUM(impact_minutes)                                         AS outage_minutes
    FROM {{ ref('slv_outage_impact') }}
    GROUP BY subscription_id
),

subscriptions AS (
    SELECT
        s.run_month,
        s.account_id,
        s.subscription_id,
        s.status,
        ZEROIFNULL(rv.revenue_php)                                  AS revenue_php,
        ZEROIFNULL(us.data_mb_30d)                                  AS data_mb_30d,
        ch.churn_probability,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'subscription_id', s.subscription_id,
            'status', s.status,
            'activated_at', s.activated_at_utc,
            'tenure_months', s.tenure_months,
            'plan', OBJECT_CONSTRUCT('id', s.plan_id, 'name', pc.plan_name, 'contract_type', pc.contract_type,
                                     'base_price', pc.base_price_php, 'speed_mbps', pc.speed_mbps),
            'device', OBJECT_CONSTRUCT('manufacturer', dc.manufacturer, 'model', dc.model,
                                       'type', dc.device_type, 'supports_5g', dc.supports_5g),
            'contract', ct.contract,
            'partner_brand', pb.brand_name,
            'usage_30d', OBJECT_CONSTRUCT('data_mb', us.data_mb_30d, 'voice_min', us.voice_min_30d,
                                          'sms', us.sms_30d, 'active_days', us.active_days_30d),
            'revenue', OBJECT_CONSTRUCT('total_php', rv.revenue_php, 'by_source', rv.revenue_by_source),
            'recharges', OBJECT_CONSTRUCT('count', rc.recharges, 'php', rc.recharge_php, 'last_pack', rc.last_pack),
            'churn_probability', ch.churn_probability,
            'segments', sg.segments,
            'outages', OBJECT_CONSTRUCT('count', ot.outages, 'minutes', ot.outage_minutes)
        )                                                           AS sub_doc
    FROM billed_subs s
    LEFT JOIN {{ ref('slv_plan') }}          pc ON pc.plan_id = s.plan_id
    LEFT JOIN {{ ref('slv_device') }}        dc ON dc.device_id = s.device_id
    LEFT JOIN {{ ref('slv_partner_brand') }} pb ON pb.partner_brand_id = s.partner_brand_id
    LEFT JOIN {{ ref('slv_churn_score') }}   ch ON ch.subscription_id = s.subscription_id
    LEFT JOIN sub_contract ct ON ct.subscription_id = s.subscription_id AND ct.run_month = s.run_month
    LEFT JOIN sub_usage us    ON us.subscription_id = s.subscription_id AND us.run_month = s.run_month
    LEFT JOIN sub_revenue rv  ON rv.subscription_id = s.subscription_id AND rv.run_month = s.run_month
    LEFT JOIN sub_segments sg ON sg.subscription_id = s.subscription_id AND sg.run_month = s.run_month
    LEFT JOIN sub_recharge rc ON rc.subscription_id = s.subscription_id AND rc.run_month = s.run_month
    LEFT JOIN sub_outage ot   ON ot.subscription_id = s.subscription_id
),

subs_agg AS (
    SELECT
        run_month,
        account_id,
        ARRAY_AGG(sub_doc) WITHIN GROUP (ORDER BY subscription_id)  AS subs,
        COUNT(*)                                                    AS subscriptions,
        COUNT_IF(status = 'ACTIVE')                                 AS active_subscriptions,
        SUM(revenue_php)                                            AS revenue_php,
        SUM(data_mb_30d)                                            AS data_mb_30d,
        MAX(churn_probability)                                      AS max_churn_probability
    FROM subscriptions
    GROUP BY run_month, account_id
),

/* ---------- 4. payments, tickets, NPS at account level ---------- */
payments AS (
    SELECT
        ba.run_month,
        pm.account_id,
        ARRAY_SLICE(
            ARRAY_AGG(OBJECT_CONSTRUCT('paid_at', pm.paid_at_local, 'amount', pm.payment_amount_php, 'method', pm.payment_method))
                WITHIN GROUP (ORDER BY pm.paid_at_local DESC), 0, 6) AS last_payments,
        MAX_BY(pm.payment_amount_php, pm.paid_at_local)             AS last_amount,
        MAX(pm.paid_at_local)                                       AS last_paid_at,
        SUM(pm.payment_amount_php)                                  AS paid_12m_php
    FROM billed_accounts ba
    JOIN params p ON p.run_month = ba.run_month
    JOIN {{ ref('slv_payment') }} pm
      ON pm.account_id = ba.account_id
     AND pm.payment_date <  p.next_month
     AND pm.payment_date >= DATEADD('month', -12, p.next_month)
    GROUP BY ba.run_month, pm.account_id
),

tickets AS (
    SELECT
        s.run_month,
        s.account_id,
        COUNT_IF(t.is_open)                                         AS open_tickets,
        COUNT_IF(t.opened_date >= DATEADD('day', -89, p.month_end)) AS tickets_90d,
        ARRAY_SLICE(
            ARRAY_AGG(OBJECT_CONSTRUCT('ticket_id', t.ticket_id, 'opened_at', t.opened_at_local, 'category', t.category,
                                       'priority', t.priority, 'status', t.status))
                WITHIN GROUP (ORDER BY t.opened_at_local DESC), 0, 3) AS latest_tickets
    FROM billed_subs s
    JOIN params p ON p.run_month = s.run_month
    JOIN {{ ref('slv_ticket') }} t
      ON t.subscription_id = s.subscription_id
     AND t.opened_date < p.next_month
    GROUP BY s.run_month, s.account_id
),

nps AS (
    SELECT
        s.run_month,
        s.account_id,
        MAX_BY(n.nps_score, n.responded_at_utc)                     AS last_nps,
        MAX_BY(OBJECT_CONSTRUCT('q_price', n.q_price, 'q_speed', n.q_speed, 'q_support', n.q_support,
                                'q_coverage', n.q_coverage, 'would_recommend', n.would_recommend),
               n.responded_at_utc)                                  AS last_nps_answers,
        AVG(n.nps_score)                                            AS avg_nps
    FROM billed_subs s
    JOIN params p ON p.run_month = s.run_month
    JOIN {{ ref('slv_nps_survey') }} n
      ON n.subscription_id = s.subscription_id
     AND n.response_date < p.next_month
    GROUP BY s.run_month, s.account_id
),

/* ---------- 5. the document ---------- */
docs AS (
    SELECT
        c.run_month                                                 AS doc_month,
        c.account_id,
        c.customer_id,
        c.customer_code,
        ZEROIFNULL(sa.subscriptions)                                AS subscriptions,
        ZEROIFNULL(sa.active_subscriptions)                         AS active_subscriptions,
        ZEROIFNULL(sa.revenue_php)                                  AS revenue_php,
        ZEROIFNULL(sa.data_mb_30d)                                  AS data_mb_30d,
        sa.max_churn_probability,
        isum.invoices,
        isum.invoiced_php,
        isum.invoice_status,
        b.bill_total,
        py.last_paid_at,
        py.last_amount,
        ZEROIFNULL(tk.open_tickets)                                 AS open_tickets,
        ZEROIFNULL(tk.tickets_90d)                                  AS tickets_90d,
        np.last_nps,
        OBJECT_CONSTRUCT_KEEP_NULL(
            'account', OBJECT_CONSTRUCT('account_id', c.account_id, 'payment_method', c.payment_method,
                                        'paperless', c.is_paperless_billing, 'billing_cycle_day', c.billing_cycle_day),
            'customer', OBJECT_CONSTRUCT('customer_code', c.customer_code, 'gender', c.gender, 'age', c.age,
                                         'senior', c.is_senior, 'partner', c.has_partner, 'dependents', c.has_dependents,
                                         'language', c.preferred_language, 'kyc', c.kyc_safe),
            'consents', ca.consents,
            'subscriptions', sa.subs,
            'bill', OBJECT_INSERT(COALESCE(b.bill_lines, OBJECT_CONSTRUCT()), 'total', b.bill_total),
            'invoice', OBJECT_CONSTRUCT('count', isum.invoices, 'amount', isum.invoiced_php,
                                        'due_date', isum.due_date, 'status', isum.invoice_status),
            'payments', OBJECT_CONSTRUCT('last', py.last_payments, 'paid_12m', py.paid_12m_php),
            'tickets', OBJECT_CONSTRUCT('open', ZEROIFNULL(tk.open_tickets), 'last_90_days', ZEROIFNULL(tk.tickets_90d),
                                        'latest', tk.latest_tickets),
            'nps', OBJECT_CONSTRUCT('last', np.last_nps, 'average', ROUND(np.avg_nps, 2), 'answers', np.last_nps_answers)
        )                                                           AS doc
    FROM customer c
    LEFT JOIN consents_agg ca      ON ca.customer_id = c.customer_id
    LEFT JOIN subs_agg sa          ON sa.account_id = c.account_id AND sa.run_month = c.run_month
    LEFT JOIN invoice_summary isum ON isum.account_id = c.account_id AND isum.run_month = c.run_month
    LEFT JOIN bill b               ON b.account_id = c.account_id AND b.run_month = c.run_month
    LEFT JOIN payments py          ON py.account_id = c.account_id AND py.run_month = c.run_month
    LEFT JOIN tickets tk           ON tk.account_id = c.account_id AND tk.run_month = c.run_month
    LEFT JOIN nps np               ON np.account_id = c.account_id AND np.run_month = c.run_month
    WHERE NOT c.is_test_customer
)

SELECT
    d.doc_month,
    d.account_id,
    d.customer_id,
    d.customer_code,
    d.subscriptions,
    d.active_subscriptions,
    ROUND(d.revenue_php, 2)                                         AS revenue_php,
    ROUND(d.data_mb_30d, 3)                                         AS data_mb_30d,
    d.max_churn_probability,
    d.invoices,
    d.invoiced_php,
    d.invoice_status,
    d.bill_total,
    ROUND(d.invoiced_php - d.bill_total, 2)                         AS invoice_vs_lines_diff,
    d.last_paid_at,
    d.last_amount,
    d.open_tickets,
    d.tickets_90d,
    d.last_nps,
    ARRAY_SIZE(d.doc:subscriptions)                                 AS doc_subscription_count,
    ARRAY_SIZE(OBJECT_KEYS(d.doc))                                  AS doc_sections,
    LENGTH(TO_JSON(d.doc))                                          AS doc_bytes,
    d.doc,
    HASH(d.doc)                                                     AS doc_hash,
    CASE
        WHEN d.max_churn_probability >= {{ var('churn_high_threshold') }} AND d.revenue_php > 0 THEN 'RETENTION_CALL'
        WHEN d.open_tickets > 0                                     THEN 'SERVICE_FOLLOW_UP'
        WHEN d.invoice_status = 'OVERDUE'                           THEN 'COLLECTIONS'
        WHEN d.last_nps >= 9                                        THEN 'REFERRAL_OFFER'
        ELSE 'NONE'
    END                                                             AS next_best_action,
    CURRENT_TIMESTAMP()                                             AS loaded_at
FROM docs d
