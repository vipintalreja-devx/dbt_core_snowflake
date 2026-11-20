{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['usage_month'],
    on_schema_change = 'sync_all_columns',
    transient = false,
    cluster_by = ['usage_month', 'home_region'],
    tags = ['gold', 'advanced', 'h07', 'roaming', 'finance'],
    query_tag = 'dbt_telco_adv_h07'
  )
}}
{#-
  adv_roaming_fx_asof  -  Roaming charges at the as-of FX rate
  ------------------------------------------------------------------------------------------
  Ported from x_h07_roaming_asof_fx (PIPE_X.SP_X_H07_ROAMING_ASOF_FX).

  For every month in the window, converts each roaming usage row from USD to PHP at the
  latest USD rate known on or before the usage day (ASOF JOIN), and reconciles four views
  of the same money:
    1. what the usage feed billed in PHP,
    2. USD x as-of rate (and x month-average and month-end rates for comparison),
    3. roaming lines on the invoices, allocated to countries by usage share,
    4. ROAMING revenue booked in daily revenue, allocated the same way.
  Roaming calls and data sessions from the CDRs are counted alongside, and a reference
  wholesale cost comes from the partner rate card.

  Grain     : usage_month x home_region x visited_country x partner_operator.
  Window    : every month from var('history_start_date'); incremental restates the last
              loaded month and later (delete+insert on usage_month).
  Changed vs original: MERGE with merge_update_columns on one month -> delete+insert per
              month for all months (all measures restated, same result); every month-level
              parameter (FX statistics, reference rates, plan as-of) is computed per month.
              Data MB uses the silver 2^20 rule; CDR roaming country comes from silver
              call_country.
  Snowflake : ASOF JOIN ... MATCH_CONDITION, QUALIFY, RATIO_TO_REPORT, DECODE, DIV0,
              ZEROIFNULL, IFF, MODE, MAX_BY, COUNT_IF.
  Sources (12): slv_roaming_usage, ref.currency_rate (bronze, keeps the load-wins rule),
              slv_subscription, slv_cell_site, slv_plan, ref.plan_feature (bronze, effective
              dated), slv_cdr_voice, slv_cdr_data, slv_invoice, slv_invoice_line,
              slv_daily_revenue, slv_partner_rate_card.
-#}

WITH params AS (
    SELECT
        {{ f3_month_lower_bound('usage_month') }}                   AS WINDOW_START,
        7                                                           AS STALE_RATE_DAYS
),

months AS (
    SELECT DISTINCT
        DATE_TRUNC('month', u.EVENT_DATE)::DATE                     AS RUN_MONTH,
        LAST_DAY(DATE_TRUNC('month', u.EVENT_DATE))::DATE           AS MONTH_END,
        DATEADD('month', 1, DATE_TRUNC('month', u.EVENT_DATE))::DATE AS NEXT_MONTH
    FROM {{ ref('slv_roaming_usage') }} u
    CROSS JOIN params p
    WHERE u.EVENT_DATE >= p.WINDOW_START
),

/* ---------- 1. FX: one rate per day (latest load wins), plus month statistics ---------- */
fx_daily AS (
    SELECT
        r.RATE_DATE,
        r.RATE,
        r.SOURCE,
        r.LOADED_AT
    FROM {{ source('ref', 'currency_rate') }} r
    CROSS JOIN params p
    WHERE r.FROM_CURRENCY = 'USD'
      AND r.TO_CURRENCY   = 'PHP'
      AND r.RATE_DATE    >= DATEADD('day', -45, p.WINDOW_START)
      AND r.RATE > 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.RATE_DATE ORDER BY r.LOADED_AT DESC) = 1
),

fx_month AS (
    SELECT
        m.RUN_MONTH,
        AVG(IFF(f.RATE_DATE BETWEEN m.RUN_MONTH AND m.MONTH_END, f.RATE, NULL)) AS MONTH_AVG_RATE,
        MAX_BY(f.RATE, f.RATE_DATE)                                 AS MONTH_END_RATE,
        MIN(IFF(f.RATE_DATE BETWEEN m.RUN_MONTH AND m.MONTH_END, f.RATE, NULL)) AS MONTH_MIN_RATE,
        MAX(IFF(f.RATE_DATE BETWEEN m.RUN_MONTH AND m.MONTH_END, f.RATE, NULL)) AS MONTH_MAX_RATE,
        COUNT_IF(f.RATE_DATE BETWEEN m.RUN_MONTH AND m.MONTH_END)   AS RATE_DAYS_IN_MONTH
    FROM months m
    JOIN fx_daily f
      ON f.RATE_DATE >= DATEADD('day', -45, m.RUN_MONTH)
     AND f.RATE_DATE <= m.MONTH_END
    GROUP BY m.RUN_MONTH
),

/* ---------- 2. roaming usage of the window, with the as-of rate ---------- */
usage_rows AS (
    SELECT
        DATE_TRUNC('month', u.EVENT_DATE)::DATE                     AS RUN_MONTH,
        u.SUBSCRIPTION_ID,
        TRIM(u.VISITED_COUNTRY)                                     AS VISITED_COUNTRY,
        COALESCE(NULLIF(TRIM(u.PARTNER_OPERATOR), ''), 'UNKNOWN')   AS PARTNER_OPERATOR,
        u.EVENT_DATE                                                AS USAGE_DATE,
        u.DATA_MB,
        u.VOICE_MIN,
        u.SMS_COUNT,
        ZEROIFNULL(u.CHARGE_USD)                                    AS CHARGE_USD,
        ZEROIFNULL(u.CHARGE_PHP_BILLED)                             AS CHARGE_PHP
    FROM {{ ref('slv_roaming_usage') }} u
    CROSS JOIN params p
    WHERE u.EVENT_DATE >= p.WINDOW_START
),

usage_asof AS (
    SELECT
        u.*,
        f.RATE                                                      AS ASOF_RATE,
        f.RATE_DATE                                                 AS ASOF_RATE_DATE,
        DATEDIFF('day', f.RATE_DATE, u.USAGE_DATE)                  AS RATE_AGE_DAYS
    FROM usage_rows u
    ASOF JOIN fx_daily f
        MATCH_CONDITION (u.USAGE_DATE >= f.RATE_DATE)
),

/* ---------- 3. subscriber home region and plan (per month) ---------- */
sub_months AS (
    SELECT DISTINCT RUN_MONTH, SUBSCRIPTION_ID FROM usage_rows
),

subscriber AS (
    SELECT
        sm.RUN_MONTH,
        s.SUBSCRIPTION_ID,
        s.ACCOUNT_ID,
        s.PLAN_ID,
        COALESCE(cs.REGION_NAME, 'UNKNOWN')                         AS HOME_REGION,
        cs.REGION_ID                                                AS HOME_REGION_ID,
        pc.PLAN_NAME,
        pc.CONTRACT_TYPE,
        IFF(pf.IS_INCLUDED, TRUE, FALSE)                            AS PLAN_INCLUDES_ROAMING,
        pf.FEATURE_VALUE                                            AS ROAMING_FEATURE_VALUE
    FROM sub_months sm
    JOIN {{ ref('slv_subscription') }} s
      ON s.SUBSCRIPTION_ID = sm.SUBSCRIPTION_ID
    LEFT JOIN {{ ref('slv_cell_site') }} cs
           ON cs.CELL_ID = s.HOME_CELL_ID
    LEFT JOIN {{ ref('slv_plan') }} pc
           ON pc.PLAN_ID = s.PLAN_ID
    LEFT JOIN {{ source('ref', 'plan_feature') }} pf
           ON pf.PLAN_ID = s.PLAN_ID
          AND pf.FEATURE_CODE = 'ROAMING'
          AND LAST_DAY(sm.RUN_MONTH) BETWEEN pf.EFFECTIVE_FROM AND COALESCE(pf.EFFECTIVE_TO, '9999-12-31'::DATE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sm.RUN_MONTH, s.SUBSCRIPTION_ID ORDER BY pf.EFFECTIVE_FROM DESC NULLS LAST) = 1
),

/* ---------- 4. roaming activity seen in the CDRs ---------- */
cdr_calls AS (
    SELECT
        DATE_TRUNC('month', c.EVENT_DATE)::DATE                     AS RUN_MONTH,
        c.SUBSCRIPTION_ID,
        UPPER(TRIM(c.CALL_COUNTRY))                                 AS VISITED_COUNTRY,
        COUNT(*)                                                    AS CALLS,
        ROUND(SUM(c.DURATION_SEC) / 60, 2)                          AS CALL_MIN
    FROM {{ ref('slv_cdr_voice') }} c
    CROSS JOIN params p
    WHERE c.IS_ROAMING
      AND c.EVENT_DATE >= p.WINDOW_START
      AND c.CALL_COUNTRY IS NOT NULL
    GROUP BY 1, 2, 3
),

cdr_data AS (
    SELECT
        DATE_TRUNC('month', d.EVENT_DATE)::DATE                     AS RUN_MONTH,
        d.SUBSCRIPTION_ID,
        ROUND(SUM(d.DATA_MB), 3)                                    AS DATA_MB,
        COUNT(*)                                                    AS SESSIONS
    FROM {{ ref('slv_cdr_data') }} d
    CROSS JOIN params p
    WHERE d.IS_ROAMING
      AND d.EVENT_DATE >= p.WINDOW_START
    GROUP BY 1, 2
),

/* data sessions carry no country: split them by the subscriber's usage-feed MB per country */
sub_country_share AS (
    SELECT
        RUN_MONTH,
        SUBSCRIPTION_ID,
        VISITED_COUNTRY,
        RATIO_TO_REPORT(SUM(DATA_MB)) OVER (PARTITION BY RUN_MONTH, SUBSCRIPTION_ID) AS MB_SHARE
    FROM usage_rows
    GROUP BY RUN_MONTH, SUBSCRIPTION_ID, VISITED_COUNTRY
),

/* ---------- 5. invoices and booked revenue (account / subscriber level) ---------- */
invoice_roaming AS (
    SELECT
        i.BILLING_MONTH                                             AS RUN_MONTH,
        i.ACCOUNT_ID,
        SUM(l.LINE_AMOUNT_PHP)                                      AS INVOICE_ROAMING_PHP
    FROM {{ ref('slv_invoice') }} i
    JOIN {{ ref('slv_invoice_line') }} l
      ON l.INVOICE_ID = i.INVOICE_ID
    CROSS JOIN params p
    WHERE i.BILLING_MONTH >= p.WINDOW_START
      AND l.LINE_TYPE = 'ROAMING'
      AND i.INVOICE_STATUS <> 'CANCELLED'
    GROUP BY 1, 2
),

revenue_roaming AS (
    SELECT
        DATE_TRUNC('month', r.BUSINESS_DATE)::DATE                  AS RUN_MONTH,
        r.SUBSCRIPTION_ID,
        SUM(r.REVENUE_PHP)                                          AS REVENUE_ROAMING_PHP
    FROM {{ ref('slv_daily_revenue') }} r
    CROSS JOIN params p
    WHERE r.REVENUE_SOURCE = 'ROAMING'
      AND r.BUSINESS_DATE >= p.WINDOW_START
    GROUP BY 1, 2
),

/* subscriber share of its account's roaming charges, used to split invoice amounts */
account_split AS (
    SELECT
        u.RUN_MONTH,
        s.ACCOUNT_ID,
        u.SUBSCRIPTION_ID,
        RATIO_TO_REPORT(SUM(u.CHARGE_PHP)) OVER (PARTITION BY u.RUN_MONTH, s.ACCOUNT_ID) AS ACCOUNT_SHARE
    FROM usage_rows u
    JOIN subscriber s
      ON s.SUBSCRIPTION_ID = u.SUBSCRIPTION_ID
     AND s.RUN_MONTH = u.RUN_MONTH
    GROUP BY u.RUN_MONTH, s.ACCOUNT_ID, u.SUBSCRIPTION_ID
),

/* ---------- 6. reference wholesale rates (PHP per unit), as of each month end ---------- */
ref_rates AS (
    SELECT
        m.RUN_MONTH,
        AVG(IFF(UPPER(rc.UNIT) = 'MB',  rc.RATE_PHP, NULL))         AS RATE_PER_MB,
        AVG(IFF(UPPER(rc.UNIT) = 'MIN', rc.RATE_PHP, NULL))         AS RATE_PER_MIN,
        AVG(IFF(UPPER(rc.UNIT) = 'MSG', rc.RATE_PHP, NULL))         AS RATE_PER_MSG
    FROM months m
    JOIN {{ ref('slv_partner_rate_card') }} rc
      ON rc.SERVICE_TYPE LIKE 'ROAM%'
     AND m.MONTH_END BETWEEN rc.EFFECTIVE_FROM AND rc.EFFECTIVE_TO
    GROUP BY m.RUN_MONTH
),

/* ---------- 7. per subscriber x country, then roll up ---------- */
sub_country AS (
    SELECT
        u.RUN_MONTH,
        u.SUBSCRIPTION_ID,
        u.VISITED_COUNTRY,
        u.PARTNER_OPERATOR,
        COUNT(*)                                                    AS USAGE_ROWS,
        SUM(u.DATA_MB)                                              AS DATA_MB,
        SUM(u.VOICE_MIN)                                            AS VOICE_MIN,
        SUM(u.SMS_COUNT)                                            AS SMS_COUNT,
        SUM(u.CHARGE_USD)                                           AS CHARGE_USD,
        SUM(u.CHARGE_PHP)                                           AS BILLED_PHP,
        SUM(u.CHARGE_USD * u.ASOF_RATE)                             AS ASOF_PHP,
        AVG(u.RATE_AGE_DAYS)                                        AS AVG_RATE_AGE_DAYS,
        MAX(u.RATE_AGE_DAYS)                                        AS MAX_RATE_AGE_DAYS,
        COUNT_IF(u.RATE_AGE_DAYS > 7)                               AS STALE_RATE_ROWS,
        COUNT_IF(u.ASOF_RATE IS NULL)                               AS ROWS_WITHOUT_RATE
    FROM usage_asof u
    GROUP BY u.RUN_MONTH, u.SUBSCRIPTION_ID, u.VISITED_COUNTRY, u.PARTNER_OPERATOR
),

enriched AS (
    SELECT
        sc.*,
        s.ACCOUNT_ID,
        s.HOME_REGION,
        s.PLAN_NAME,
        s.PLAN_INCLUDES_ROAMING,
        ZEROIFNULL(cc.CALLS)                                        AS CDR_CALLS,
        ZEROIFNULL(cc.CALL_MIN)                                     AS CDR_CALL_MIN,
        ROUND(ZEROIFNULL(cd.DATA_MB) * ZEROIFNULL(sh.MB_SHARE), 3)  AS CDR_DATA_MB,
        RATIO_TO_REPORT(sc.BILLED_PHP) OVER (PARTITION BY sc.RUN_MONTH, sc.SUBSCRIPTION_ID) AS SUB_SHARE,
        ZEROIFNULL(asp.ACCOUNT_SHARE)                               AS ACCOUNT_SHARE,
        ZEROIFNULL(ir.INVOICE_ROAMING_PHP)                          AS ACCOUNT_INVOICE_ROAMING_PHP,
        ZEROIFNULL(rr.REVENUE_ROAMING_PHP)                          AS SUB_REVENUE_ROAMING_PHP
    FROM sub_country sc
    LEFT JOIN subscriber s         ON s.SUBSCRIPTION_ID = sc.SUBSCRIPTION_ID AND s.RUN_MONTH = sc.RUN_MONTH
    LEFT JOIN cdr_calls cc         ON cc.SUBSCRIPTION_ID = sc.SUBSCRIPTION_ID AND cc.RUN_MONTH = sc.RUN_MONTH
                                  AND cc.VISITED_COUNTRY = sc.VISITED_COUNTRY
    LEFT JOIN cdr_data cd          ON cd.SUBSCRIPTION_ID = sc.SUBSCRIPTION_ID AND cd.RUN_MONTH = sc.RUN_MONTH
    LEFT JOIN sub_country_share sh ON sh.SUBSCRIPTION_ID = sc.SUBSCRIPTION_ID AND sh.RUN_MONTH = sc.RUN_MONTH
                                  AND sh.VISITED_COUNTRY = sc.VISITED_COUNTRY
    LEFT JOIN account_split asp    ON asp.SUBSCRIPTION_ID = sc.SUBSCRIPTION_ID AND asp.RUN_MONTH = sc.RUN_MONTH
                                  AND asp.ACCOUNT_ID = s.ACCOUNT_ID
    LEFT JOIN invoice_roaming ir   ON ir.ACCOUNT_ID = s.ACCOUNT_ID AND ir.RUN_MONTH = sc.RUN_MONTH
    LEFT JOIN revenue_roaming rr   ON rr.SUBSCRIPTION_ID = sc.SUBSCRIPTION_ID AND rr.RUN_MONTH = sc.RUN_MONTH
),

rolled AS (
    SELECT
        e.RUN_MONTH,
        COALESCE(e.HOME_REGION, 'UNKNOWN')                          AS HOME_REGION,
        e.VISITED_COUNTRY,
        e.PARTNER_OPERATOR,
        COUNT(DISTINCT e.SUBSCRIPTION_ID)                           AS ROAMERS,
        SUM(e.USAGE_ROWS)                                           AS USAGE_ROWS,
        SUM(e.DATA_MB)                                              AS DATA_MB,
        SUM(e.VOICE_MIN)                                            AS VOICE_MIN,
        SUM(e.SMS_COUNT)                                            AS SMS_COUNT,
        SUM(e.CHARGE_USD)                                           AS CHARGE_USD,
        SUM(e.BILLED_PHP)                                           AS BILLED_PHP,
        SUM(e.ASOF_PHP)                                             AS ASOF_PHP,
        SUM(e.AVG_RATE_AGE_DAYS * e.USAGE_ROWS) / NULLIF(SUM(e.USAGE_ROWS), 0) AS AVG_RATE_AGE_DAYS,
        MAX(e.MAX_RATE_AGE_DAYS)                                    AS MAX_RATE_AGE_DAYS,
        SUM(e.STALE_RATE_ROWS)                                      AS STALE_RATE_ROWS,
        SUM(e.ROWS_WITHOUT_RATE)                                    AS ROWS_WITHOUT_RATE,
        SUM(e.CDR_CALLS)                                            AS CDR_CALLS,
        SUM(e.CDR_CALL_MIN)                                         AS CDR_CALL_MIN,
        SUM(e.CDR_DATA_MB)                                          AS CDR_DATA_MB,
        SUM(e.ACCOUNT_INVOICE_ROAMING_PHP * e.ACCOUNT_SHARE * e.SUB_SHARE) AS ALLOCATED_INVOICE_PHP,
        SUM(e.SUB_REVENUE_ROAMING_PHP * e.SUB_SHARE)                AS ALLOCATED_REVENUE_PHP,
        COUNT_IF(e.PLAN_INCLUDES_ROAMING)                           AS ROAMERS_WITH_ROAMING_PLAN,
        MODE(e.PLAN_NAME)                                           AS TOP_PLAN_NAME
    FROM enriched e
    GROUP BY 1, 2, 3, 4
)

SELECT
    r.RUN_MONTH                                                     AS USAGE_MONTH,
    r.HOME_REGION,
    r.VISITED_COUNTRY,
    DECODE(r.VISITED_COUNTRY,
           'US', 'USD', 'SG', 'SGD', 'HK', 'HKD', 'JP', 'JPY', 'AU', 'AUD',
           'GB', 'GBP', 'IN', 'INR', 'AE', 'AED', 'OTHER')         AS LOCAL_CURRENCY,
    DECODE(r.VISITED_COUNTRY,
           'US', 'AMERICAS', 'SG', 'ASIA', 'HK', 'ASIA', 'JP', 'ASIA', 'IN', 'ASIA',
           'AU', 'OCEANIA', 'GB', 'EUROPE', 'AE', 'MIDDLE_EAST', 'OTHER') AS DESTINATION_ZONE,
    r.PARTNER_OPERATOR,
    r.ROAMERS,
    r.USAGE_ROWS,
    ROUND(r.DATA_MB, 3)                                             AS DATA_MB,
    ROUND(r.VOICE_MIN, 2)                                           AS VOICE_MIN,
    r.SMS_COUNT,
    ROUND(r.CHARGE_USD, 4)                                          AS CHARGE_USD,
    ROUND(r.BILLED_PHP, 2)                                          AS BILLED_PHP,
    ROUND(r.ASOF_PHP, 2)                                            AS ASOF_PHP,
    ROUND(r.CHARGE_USD * fm.MONTH_AVG_RATE, 2)                      AS MONTH_AVG_PHP,
    ROUND(r.CHARGE_USD * fm.MONTH_END_RATE, 2)                      AS MONTH_END_PHP,
    ROUND(r.BILLED_PHP - r.ASOF_PHP, 2)                             AS DIFF_ASOF_PHP,
    ROUND(DIV0(r.BILLED_PHP - r.ASOF_PHP, r.ASOF_PHP) * 100, 4)     AS DIFF_PCT,
    ROUND(r.AVG_RATE_AGE_DAYS, 2)                                   AS AVG_RATE_AGE_DAYS,
    r.MAX_RATE_AGE_DAYS,
    r.STALE_RATE_ROWS,
    r.ROWS_WITHOUT_RATE,
    ROUND(fm.MONTH_MIN_RATE, 6)                                     AS MONTH_MIN_RATE,
    ROUND(fm.MONTH_MAX_RATE, 6)                                     AS MONTH_MAX_RATE,
    fm.RATE_DAYS_IN_MONTH,
    r.CDR_CALLS,
    r.CDR_CALL_MIN,
    r.CDR_DATA_MB,
    ROUND(DIV0(r.CDR_CALL_MIN, r.VOICE_MIN), 4)                     AS CDR_TO_FEED_MIN_RATIO,
    ROUND(r.ALLOCATED_INVOICE_PHP, 2)                               AS ALLOCATED_INVOICE_PHP,
    ROUND(r.ALLOCATED_REVENUE_PHP, 2)                               AS ALLOCATED_REVENUE_PHP,
    ROUND(r.DATA_MB * ZEROIFNULL(rr.RATE_PER_MB)
        + r.VOICE_MIN * ZEROIFNULL(rr.RATE_PER_MIN)
        + r.SMS_COUNT * ZEROIFNULL(rr.RATE_PER_MSG), 2)             AS REFERENCE_COST_PHP,
    ROUND(r.BILLED_PHP - (r.DATA_MB * ZEROIFNULL(rr.RATE_PER_MB)
        + r.VOICE_MIN * ZEROIFNULL(rr.RATE_PER_MIN)
        + r.SMS_COUNT * ZEROIFNULL(rr.RATE_PER_MSG)), 2)            AS MARGIN_PHP,
    CASE
        WHEN r.ROWS_WITHOUT_RATE > 0                                THEN 'MISSING_RATE'
        WHEN ABS(DIV0(r.BILLED_PHP - r.ASOF_PHP, r.ASOF_PHP)) > 0.02 THEN 'FX_MISMATCH'
        WHEN r.STALE_RATE_ROWS > 0                                  THEN 'STALE_RATE'
        WHEN ABS(r.ALLOCATED_INVOICE_PHP - r.BILLED_PHP) > 1        THEN 'INVOICE_GAP'
        ELSE 'OK'
    END                                                             AS RECON_STATUS,
    ROUND(DIV0(r.ROAMERS_WITH_ROAMING_PLAN, r.ROAMERS), 4)          AS ROAMING_PLAN_SHARE,
    r.TOP_PLAN_NAME,
    RANK() OVER (PARTITION BY r.RUN_MONTH, r.HOME_REGION ORDER BY r.BILLED_PHP DESC) AS RANK_IN_REGION,
    ROUND(RATIO_TO_REPORT(r.BILLED_PHP) OVER (PARTITION BY r.RUN_MONTH), 6)          AS SHARE_OF_MONTH,
    CURRENT_TIMESTAMP()                                             AS LOADED_AT
FROM rolled r
LEFT JOIN fx_month fm  ON fm.RUN_MONTH = r.RUN_MONTH
LEFT JOIN ref_rates rr ON rr.RUN_MONTH = r.RUN_MONTH
