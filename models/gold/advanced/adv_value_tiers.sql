{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'value_month',
    on_schema_change = 'append_new_columns',
    transient = true,
    cluster_by = ['value_month', 'value_tier'],
    tags = ['gold', 'advanced', 'h13', 'value', 'customer'],
    query_tag = 'dbt_telco_h13'
  )
}}
{#-
  adv_value_tiers  -  Customer value tiers (RFM + payment discipline + engagement + risk)
  ------------------------------------------------------------------------------------------
  Ported from x_h13_py_value_tiers (PIPE_X.SP_X_H13_PY_VALUE_TIERS, originally a Snowpark
  Python procedure using pandas qcut/cut).
  Grain : one row per subscription per value_month (lines with revenue in the month).
  Change vs. original: computed for EVERY month in the loaded window instead of one
  run_month; reads silver/gold instead of raw tables. "12M" metrics cover up to 12 months
  of the loaded history (var history_start_date).

  Snowflake-specific: NTILE, PERCENT_RANK, WIDTH_BUCKET, APPROX_PERCENTILE, RATIO_TO_REPORT,
  REGR_SLOPE, STDDEV_SAMP, DIV0, ZEROIFNULL, IFF, DECODE, QUALIFY, MAX_BY, GREATEST/LEAST.
  Refs (12): int_subscriber_month_status (spine), fct_revenue_daily, slv_subscription,
  slv_account, slv_customer, slv_plan, slv_recharge, slv_invoice, slv_payment,
  fct_subscriber_usage_daily, slv_ticket, slv_nps_survey, slv_churn_score, slv_contract.
-#}

WITH params AS (
    {{ f4_months_cte('value_month') }}
),

/* ---------- 1. revenue: month, previous month, 12 months ---------- */
revenue_monthly AS (
    SELECT
        r.subscription_id,
        DATE_TRUNC('month', r.business_date)::DATE                  AS rev_month,
        SUM(r.total_revenue_php)                                    AS revenue_php,
        SUM(r.plan_revenue_php)                                     AS plan_php,
        COUNT_IF(r.total_revenue_php <> 0)                          AS paying_days
    FROM {{ ref('fct_revenue_daily') }} r
    WHERE r.business_date >= (SELECT MIN(year_start) FROM params)
      AND r.business_date <  (SELECT MAX(next_month) FROM params)
    GROUP BY 1, 2
),

revenue_profile AS (
    SELECT
        p.run_month,
        rm.subscription_id,
        SUM(IFF(rm.rev_month = p.run_month,  rm.revenue_php, 0))    AS revenue_php,
        SUM(IFF(rm.rev_month = p.prev_month, rm.revenue_php, 0))    AS prev_revenue_php,
        SUM(IFF(rm.rev_month = p.run_month,  rm.plan_php, 0))       AS plan_revenue_php,
        SUM(rm.revenue_php)                                         AS revenue_12m_php,
        COUNT(*)                                                    AS revenue_months_12m,
        SUM(IFF(rm.rev_month = p.run_month, rm.paying_days, 0))     AS paying_days,
        STDDEV_SAMP(rm.revenue_php)                                 AS revenue_stddev_12m,
        REGR_SLOPE(rm.revenue_php, DATEDIFF('month', p.year_start, rm.rev_month)) AS revenue_trend_12m
    FROM params p
    JOIN revenue_monthly rm
      ON rm.rev_month BETWEEN p.year_start AND p.run_month
    GROUP BY p.run_month, rm.subscription_id
    HAVING SUM(IFF(rm.rev_month = p.run_month, rm.revenue_php, 0)) > 0
),

/* ---------- 2. subscriber context ---------- */
subscriber AS (
    SELECT
        s.subscription_id,
        s.account_id,
        s.plan_id,
        s.tenure_months,
        s.monthly_charges_php,
        s.internet_service,
        s.status,
        a.payment_method,
        a.is_paperless_billing,
        c.is_senior,
        c.gender,
        c.date_of_birth,
        pc.plan_name,
        pc.base_price_php
    FROM {{ ref('slv_subscription') }} s
    LEFT JOIN {{ ref('slv_account') }}  a  ON a.account_id = s.account_id
    LEFT JOIN {{ ref('slv_customer') }} c  ON c.customer_id = a.customer_id
    LEFT JOIN {{ ref('slv_plan') }}     pc ON pc.plan_id = s.plan_id
    WHERE s.is_reportable
),

/* ---------- 3. recency and frequency of paying behaviour ---------- */
recharges AS (
    SELECT
        p.run_month,
        rc.subscription_id,
        MAX(rc.recharged_at_local)                                  AS last_recharge_at,
        COUNT_IF(rc.event_date >= p.run_month)                      AS recharges_month,
        COUNT(*)                                                    AS recharges_90d,
        SUM(rc.recharge_amount_php)                                 AS recharge_php_90d
    FROM params p
    JOIN {{ ref('slv_recharge') }} rc
      ON rc.event_date >= DATEADD('day', -90, p.next_month)
     AND rc.event_date <  p.next_month
    GROUP BY 1, 2
),

payments AS (
    SELECT
        p.run_month,
        i.account_id,
        COUNT(DISTINCT i.invoice_id)                                AS invoices_6m,
        COUNT(DISTINCT IFF(pm.payment_date <= i.due_date, i.invoice_id, NULL)) AS paid_on_time_6m,
        COUNT(DISTINCT IFF(pm.payment_id IS NULL, i.invoice_id, NULL)) AS unpaid_6m,
        AVG(DATEDIFF('day', i.due_date, pm.payment_date))           AS avg_days_past_due,
        MAX(pm.paid_at_local)                                       AS last_payment_at,
        MAX_BY(pm.payment_method, pm.paid_at_local)                 AS last_payment_method
    FROM params p
    JOIN {{ ref('slv_invoice') }} i
      ON i.billing_month >= DATEADD('month', -5, p.run_month)
     AND i.billing_month <= p.run_month
    LEFT JOIN {{ ref('slv_payment') }} pm
           ON pm.invoice_id = i.invoice_id
          AND pm.payment_date < p.next_month
    GROUP BY 1, 2
),

/* ---------- 4. engagement, pain and risk ---------- */
engagement AS (
    SELECT
        DATE_TRUNC('month', u.event_date)::DATE                     AS run_month,
        u.subscription_id,
        SUM(u.data_mb)                                              AS data_mb_month,
        SUM(u.voice_min)                                            AS voice_min_month,
        COUNT(*)                                                    AS active_days_month
    FROM {{ ref('fct_subscriber_usage_daily') }} u
    WHERE DATE_TRUNC('month', u.event_date) IN (SELECT run_month FROM params)
    GROUP BY 1, 2
),

pain AS (
    SELECT
        p.run_month,
        t.subscription_id,
        COUNT(*)                                                    AS tickets_90d,
        COUNT_IF(t.priority IN ('P1', 'P2'))                        AS urgent_tickets_90d
    FROM params p
    JOIN {{ ref('slv_ticket') }} t
      ON t.opened_date >= DATEADD('day', -90, p.next_month)
     AND t.opened_date <  p.next_month
    GROUP BY 1, 2
),

nps AS (
    SELECT
        p.run_month,
        n.subscription_id,
        MAX_BY(n.nps_score, n.responded_at_utc)                     AS last_nps
    FROM params p
    JOIN {{ ref('slv_nps_survey') }} n
      ON n.response_date < p.next_month
    GROUP BY 1, 2
),

risk AS (
    SELECT subscription_id, churn_probability, score_date
    FROM {{ ref('slv_churn_score') }}
),

contract AS (
    SELECT
        p.run_month,
        ct.subscription_id,
        ct.contract_type,
        DATEDIFF('month', p.month_end, ct.end_date)                 AS contract_months_left
    FROM params p
    JOIN {{ ref('slv_contract') }} ct
      ON ct.start_date <= p.month_end
     AND (ct.terminated_on IS NULL OR ct.terminated_on > p.month_end)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.run_month, ct.subscription_id ORDER BY ct.start_date DESC) = 1
),

/* ---------- 5. RFM scoring ---------- */
features AS (
    SELECT
        rp.run_month,
        rp.subscription_id,
        sb.account_id,
        sb.plan_id,
        sb.plan_name,
        sb.base_price_php,
        sb.tenure_months,
        sb.monthly_charges_php,
        sb.internet_service,
        sb.status,
        sb.payment_method,
        sb.is_paperless_billing,
        sb.is_senior,
        DATEDIFF('year', sb.date_of_birth, p.month_end)             AS age,
        rp.revenue_php::FLOAT                                       AS revenue_php,
        rp.prev_revenue_php::FLOAT                                  AS prev_revenue_php,
        rp.plan_revenue_php,
        rp.revenue_12m_php,
        rp.revenue_months_12m,
        rp.paying_days,
        rp.revenue_stddev_12m,
        rp.revenue_trend_12m,
        DATEDIFF('day', GREATEST(COALESCE(rc.last_recharge_at::DATE, '1900-01-01'::DATE),
                                 COALESCE(py.last_payment_at::DATE,  '1900-01-01'::DATE)),
                 p.month_end)                                       AS days_since_last_pay,
        ZEROIFNULL(rc.recharges_month)                              AS recharges_month,
        ZEROIFNULL(rc.recharges_90d)                                AS recharges_90d,
        ZEROIFNULL(rc.recharge_php_90d)                             AS recharge_php_90d,
        ZEROIFNULL(py.invoices_6m)                                  AS invoices_6m,
        ROUND(DIV0(py.paid_on_time_6m, py.invoices_6m), 4)          AS on_time_rate_6m,
        ZEROIFNULL(py.unpaid_6m)                                    AS unpaid_6m,
        ROUND(py.avg_days_past_due, 1)                              AS avg_days_past_due,
        py.last_payment_method,
        ZEROIFNULL(e.data_mb_month)                                 AS data_mb_month,
        ZEROIFNULL(e.voice_min_month)                               AS voice_min_month,
        ZEROIFNULL(e.active_days_month)                             AS active_days_month,
        ZEROIFNULL(pn.tickets_90d)                                  AS tickets_90d,
        ZEROIFNULL(pn.urgent_tickets_90d)                           AS urgent_tickets_90d,
        n.last_nps,
        rk.churn_probability,
        ct.contract_type,
        ct.contract_months_left
    FROM revenue_profile rp
    JOIN params p             ON p.run_month = rp.run_month
    JOIN subscriber sb        ON sb.subscription_id = rp.subscription_id
    LEFT JOIN recharges rc    ON rc.subscription_id = rp.subscription_id AND rc.run_month = rp.run_month
    LEFT JOIN payments py     ON py.account_id = sb.account_id           AND py.run_month = rp.run_month
    LEFT JOIN engagement e    ON e.subscription_id = rp.subscription_id  AND e.run_month = rp.run_month
    LEFT JOIN pain pn         ON pn.subscription_id = rp.subscription_id AND pn.run_month = rp.run_month
    LEFT JOIN nps n           ON n.subscription_id = rp.subscription_id  AND n.run_month = rp.run_month
    LEFT JOIN risk rk         ON rk.subscription_id = rp.subscription_id AND rk.score_date <= p.month_end
    LEFT JOIN contract ct     ON ct.subscription_id = rp.subscription_id AND ct.run_month = rp.run_month
),

scored AS (
    SELECT
        f.*,
        NTILE(10) OVER (PARTITION BY f.run_month ORDER BY f.revenue_php, f.subscription_id)       AS revenue_decile,
        NTILE(5)  OVER (PARTITION BY f.run_month ORDER BY f.days_since_last_pay DESC, f.subscription_id) AS r_score,
        NTILE(5)  OVER (PARTITION BY f.run_month ORDER BY f.paying_days + f.recharges_90d, f.subscription_id) AS f_score,
        NTILE(5)  OVER (PARTITION BY f.run_month ORDER BY f.revenue_12m_php, f.subscription_id)   AS m_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY f.run_month ORDER BY f.revenue_php), 6)          AS revenue_pct_rank,
        WIDTH_BUCKET(LEAST(f.tenure_months, 72), 0, 72, 6)                                        AS tenure_bucket,
        NTILE(10) OVER (PARTITION BY f.run_month ORDER BY f.prev_revenue_php, f.subscription_id)  AS prev_revenue_decile
    FROM features f
),

valued AS (
    SELECT
        s.*,
        ROUND(
              0.35 * s.m_score / 5
            + 0.20 * s.f_score / 5
            + 0.15 * s.r_score / 5
            + 0.15 * COALESCE(s.on_time_rate_6m, 0.5)
            + 0.10 * LEAST(s.active_days_month / 30, 1)
            + 0.05 * IFF(s.last_nps >= 9, 1, IFF(s.last_nps <= 6, 0, 0.5))
        , 4)                                                        AS value_score,
        CASE
            WHEN s.revenue_decile <= 5 THEN 'BRONZE'
            WHEN s.revenue_decile <= 8 THEN 'SILVER'
            WHEN s.revenue_decile = 9  THEN 'GOLD'
            ELSE 'PLATINUM'
        END                                                         AS value_tier,
        CASE
            WHEN s.prev_revenue_php = 0       THEN 'NEW'
            WHEN s.prev_revenue_decile <= 5   THEN 'BRONZE'
            WHEN s.prev_revenue_decile <= 8   THEN 'SILVER'
            WHEN s.prev_revenue_decile = 9    THEN 'GOLD'
            ELSE 'PLATINUM'
        END                                                         AS prev_value_tier
    FROM scored s
)

SELECT
    v.run_month                                                     AS value_month,
    v.subscription_id,
    v.account_id,
    v.plan_id,
    v.plan_name,
    v.status,
    ROUND(v.revenue_php, 2)                                         AS revenue_php,
    ROUND(v.prev_revenue_php, 2)                                    AS prev_revenue_php,
    ROUND(v.plan_revenue_php, 2)                                    AS plan_revenue_php,
    ROUND(v.revenue_12m_php, 2)                                     AS revenue_12m_php,
    v.revenue_months_12m,
    ROUND(v.revenue_trend_12m, 4)                                   AS revenue_trend_12m,
    ROUND(DIV0(v.revenue_stddev_12m, DIV0(v.revenue_12m_php, v.revenue_months_12m)), 4) AS revenue_volatility,
    v.revenue_decile,
    v.revenue_pct_rank,
    v.value_tier,
    v.prev_value_tier,
    CASE
        WHEN v.prev_value_tier = 'NEW'                              THEN 'NEW'
        WHEN DECODE(v.value_tier, 'BRONZE', 1, 'SILVER', 2, 'GOLD', 3, 4)
           > DECODE(v.prev_value_tier, 'BRONZE', 1, 'SILVER', 2, 'GOLD', 3, 4) THEN 'UP'
        WHEN DECODE(v.value_tier, 'BRONZE', 1, 'SILVER', 2, 'GOLD', 3, 4)
           < DECODE(v.prev_value_tier, 'BRONZE', 1, 'SILVER', 2, 'GOLD', 3, 4) THEN 'DOWN'
        ELSE 'SAME'
    END                                                             AS tier_movement,
    v.r_score,
    v.f_score,
    v.m_score,
    v.r_score::STRING || v.f_score::STRING || v.m_score::STRING     AS rfm_code,
    v.value_score,
    v.days_since_last_pay,
    v.paying_days,
    v.recharges_month,
    v.recharges_90d,
    ROUND(v.recharge_php_90d, 2)                                    AS recharge_php_90d,
    v.invoices_6m,
    v.on_time_rate_6m,
    v.unpaid_6m,
    v.avg_days_past_due,
    v.last_payment_method,
    ROUND(v.data_mb_month, 3)                                       AS data_mb_month,
    ROUND(v.voice_min_month, 2)                                     AS voice_min_month,
    v.active_days_month,
    v.tickets_90d,
    v.urgent_tickets_90d,
    v.last_nps,
    v.churn_probability,
    v.contract_type,
    v.contract_months_left,
    v.tenure_months,
    v.tenure_bucket,
    CASE
        WHEN v.tenure_months IS NULL THEN 'UNKNOWN'
        WHEN v.tenure_months <= 12   THEN '0-12'
        WHEN v.tenure_months <= 36   THEN '13-36'
        ELSE '37+'
    END                                                             AS tenure_band,
    v.age,
    v.is_senior,
    v.payment_method,
    v.is_paperless_billing,
    v.internet_service,
    ROUND(DIV0(v.revenue_12m_php, v.revenue_months_12m)
          * LEAST(DIV0(1, GREATEST(COALESCE(v.churn_probability, 0.05) / 12, 0.005)), 60), 2) AS est_lifetime_value_php,
    ROUND(RATIO_TO_REPORT(v.revenue_php) OVER (PARTITION BY v.run_month, v.value_tier), 8)   AS share_of_tier_revenue,
    APPROX_PERCENTILE(v.revenue_php, 0.5) OVER (PARTITION BY v.run_month, v.plan_id)          AS plan_median_revenue_php,
    CASE
        WHEN v.value_tier IN ('GOLD', 'PLATINUM') AND COALESCE(v.churn_probability, 0) >= 0.5 THEN 'PROTECT'
        WHEN v.value_tier IN ('GOLD', 'PLATINUM')                                            THEN 'REWARD'
        WHEN v.value_tier = 'SILVER' AND v.revenue_trend_12m > 0                             THEN 'GROW'
        WHEN v.unpaid_6m >= 2                                                               THEN 'COLLECT'
        ELSE 'MAINTAIN'
    END                                                             AS recommended_action,
    CURRENT_TIMESTAMP()                                             AS loaded_at
FROM valued v
