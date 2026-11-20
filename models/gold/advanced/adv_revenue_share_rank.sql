{{
  config(
    materialized = 'view',
    secure = true,
    tags = ['gold', 'advanced', 'h08', 'revenue'],
    query_tag = 'dbt_telco_adv_h08'
  )
}}
{#-
  adv_revenue_share_rank  -  Revenue share and rank of plans per region
  ------------------------------------------------------------------------------------------
  Ported from x_h08_revenue_share_rank (PIPE_X.SP_X_H08_REVENUE_SHARE_RANK).

  Top 10 plans per home region by revenue for every month in the window, with each plan's
  share of the region, rank, cumulative distribution and decile, month-over-month change,
  revenue split by source (PIVOT), collection rate on invoices, usage per subscriber,
  churn exposure, feature richness and partner mix.

  Grain     : revenue_month x region_name x plan_id (top 10 plans per region and month).
  Window    : every month present in slv_daily_revenue (the silver reporting window). The
              first month has no previous month in silver, so its MoM growth is NULL.
  Changed vs original: single RUN_MONTH -> all months (every window/rank partition now also
              partitions by month; previous-month revenue comes from LAG over months instead
              of a CUR/PREV period flag). Still a SECURE VIEW as in the original. Note kept
              from the original: an invoice is joined to every line of its account, so
              INVOICED_PLAN_FEE_PHP / PAID_PHP over-count multi-line accounts.
  Snowflake : PIVOT, RATIO_TO_REPORT, RANK / DENSE_RANK / CUME_DIST / NTILE, QUALIFY, LAG,
              MEDIAN (window), DIV0, ZEROIFNULL, IFF, ARRAY_AGG ... WITHIN GROUP,
              ARRAY_COMPACT, ARRAY_TO_STRING, APPROX_COUNT_DISTINCT, MODE, secure view.
  Sources (15): slv_daily_revenue, slv_subscription, slv_cell_site, ref.plan_catalog
              (bronze: channels attribute), ref.plan_feature (bronze, effective dated),
              slv_partner_brand, slv_contract, slv_recharge, fct_subscriber_usage_daily,
              slv_churn_score, slv_segment_membership, slv_invoice, slv_invoice_line,
              slv_payment, slv_plan.
-#}

WITH months AS (
    SELECT
        RUN_MONTH,
        LAST_DAY(RUN_MONTH)                                         AS MONTH_END,
        DATEADD('month', 1, RUN_MONTH)                              AS NEXT_MONTH,
        DATEADD('month', -1, RUN_MONTH)                             AS PREV_MONTH
    FROM (
        SELECT DISTINCT DATE_TRUNC('month', BUSINESS_DATE)::DATE    AS RUN_MONTH
        FROM {{ ref('slv_daily_revenue') }}
    )
),

/* ---------- 1. subscriber attributes ---------- */
subscriber AS (
    SELECT
        s.SUBSCRIPTION_ID,
        s.ACCOUNT_ID,
        s.PLAN_ID,
        s.PARTNER_BRAND_ID,
        s.MONTHLY_CHARGES_PHP                                       AS MONTHLY_CHARGES,
        COALESCE(cs.REGION_NAME, 'UNKNOWN')                         AS REGION_NAME,
        cs.REGION_ID,
        s.STATUS
    FROM {{ ref('slv_subscription') }} s
    LEFT JOIN {{ ref('slv_cell_site') }} cs
           ON cs.CELL_ID = s.HOME_CELL_ID
    WHERE NOT s.IS_DELETED
),

/* ---------- 2. revenue per month and source ---------- */
revenue_by_source AS (
    SELECT
        DATE_TRUNC('month', r.BUSINESS_DATE)::DATE                  AS RUN_MONTH,
        sb.REGION_NAME,
        sb.PLAN_ID,
        r.REVENUE_SOURCE,
        SUM(r.REVENUE_PHP)                                          AS REVENUE_PHP,
        COUNT(DISTINCT r.SUBSCRIPTION_ID)                           AS PAYING_SUBS
    FROM {{ ref('slv_daily_revenue') }} r
    JOIN subscriber sb
      ON sb.SUBSCRIPTION_ID = r.SUBSCRIPTION_ID
    GROUP BY 1, 2, 3, 4
),

revenue_pivot AS (
    SELECT *
    FROM (
        SELECT RUN_MONTH, REGION_NAME, PLAN_ID, REVENUE_SOURCE, REVENUE_PHP
        FROM revenue_by_source
    )
    PIVOT (SUM(REVENUE_PHP) FOR REVENUE_SOURCE IN ('PLAN', 'VAS', 'RECHARGE', 'ROAMING'))
        AS pv (RUN_MONTH, REGION_NAME, PLAN_ID, REV_PLAN, REV_VAS, REV_RECHARGE, REV_ROAMING)
),

revenue_month AS (
    SELECT
        RUN_MONTH,
        REGION_NAME,
        PLAN_ID,
        SUM(REVENUE_PHP)                                            AS REVENUE_PHP,
        MAX(PAYING_SUBS)                                            AS PAYING_SUBS
    FROM revenue_by_source
    GROUP BY RUN_MONTH, REGION_NAME, PLAN_ID
),

revenue_totals AS (
    SELECT
        rm.*,
        LAG(rm.REVENUE_PHP) OVER (PARTITION BY rm.REGION_NAME, rm.PLAN_ID ORDER BY rm.RUN_MONTH) AS LAG_REVENUE_PHP,
        LAG(rm.RUN_MONTH)   OVER (PARTITION BY rm.REGION_NAME, rm.PLAN_ID ORDER BY rm.RUN_MONTH) AS LAG_MONTH
    FROM revenue_month rm
),

/* ---------- 3. plan facts (as of each month end) ---------- */
plan_info AS (
    SELECT
        m.RUN_MONTH,
        pc.PLAN_ID,
        pc.PLAN_NAME,
        pc.CONTRACT_TYPE,
        pc.INTERNET_SERVICE,
        pc.BASE_PRICE,
        pc.ATTRIBUTES:speed_mbps::NUMBER                            AS SPEED_MBPS,
        pc.ATTRIBUTES:data_cap_gb::NUMBER                           AS DATA_CAP_GB,
        ARRAY_TO_STRING(pc.ATTRIBUTES:channels, ',')                AS SALES_CHANNELS
    FROM months m
    JOIN {{ source('ref', 'plan_catalog') }} pc
      ON m.MONTH_END BETWEEN pc.EFFECTIVE_FROM AND COALESCE(pc.EFFECTIVE_TO, '9999-12-31'::DATE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY m.RUN_MONTH, pc.PLAN_ID ORDER BY pc.EFFECTIVE_FROM DESC) = 1
),

plan_current AS (
    SELECT PLAN_ID, PLAN_NAME, CONTRACT_TYPE, INTERNET_SERVICE, BASE_PRICE_PHP, SPEED_MBPS, DATA_CAP_GB
    FROM {{ ref('slv_plan') }}
),

plan_features AS (
    SELECT
        m.RUN_MONTH,
        pf.PLAN_ID,
        COUNT_IF(pf.IS_INCLUDED)                                    AS FEATURES_INCLUDED,
        ARRAY_AGG(IFF(pf.IS_INCLUDED, pf.FEATURE_CODE, NULL)) WITHIN GROUP (ORDER BY pf.FEATURE_CODE) AS FEATURE_LIST,
        MAX(IFF(pf.FEATURE_CODE = 'STREAMING' AND pf.IS_INCLUDED, 1, 0)) AS HAS_STREAMING,
        MAX(IFF(pf.FEATURE_CODE = 'ROAMING' AND pf.IS_INCLUDED, 1, 0))   AS HAS_ROAMING
    FROM months m
    JOIN {{ source('ref', 'plan_feature') }} pf
      ON m.MONTH_END BETWEEN pf.EFFECTIVE_FROM AND COALESCE(pf.EFFECTIVE_TO, '9999-12-31'::DATE)
    GROUP BY m.RUN_MONTH, pf.PLAN_ID
),

/* ---------- 4. subscriber base, usage, recharges, churn, contracts per region x plan ---------- */
base AS (
    SELECT
        REGION_NAME,
        PLAN_ID,
        COUNT(*)                                                    AS SUBSCRIBERS,
        COUNT_IF(STATUS = 'ACTIVE')                                 AS ACTIVE_SUBS,
        COUNT_IF(STATUS = 'SUSPENDED')                              AS SUSPENDED_SUBS,
        COUNT_IF(PARTNER_BRAND_ID IS NOT NULL)                      AS PARTNER_SUBS,
        AVG(MONTHLY_CHARGES)                                        AS AVG_MONTHLY_CHARGES
    FROM subscriber
    GROUP BY REGION_NAME, PLAN_ID
),

usage AS (
    SELECT
        DATE_TRUNC('month', u.EVENT_DATE)::DATE                     AS RUN_MONTH,
        sb.REGION_NAME,
        sb.PLAN_ID,
        SUM(u.DATA_MB)                                              AS DATA_MB,
        SUM(u.VOICE_MIN)                                            AS VOICE_MIN,
        APPROX_COUNT_DISTINCT(u.SUBSCRIPTION_ID)                    AS USING_SUBS
    FROM {{ ref('fct_subscriber_usage_daily') }} u
    JOIN subscriber sb
      ON sb.SUBSCRIPTION_ID = u.SUBSCRIPTION_ID
    GROUP BY 1, 2, 3
),

recharges AS (
    SELECT
        DATE_TRUNC('month', rc.EVENT_DATE)::DATE                    AS RUN_MONTH,
        sb.REGION_NAME,
        sb.PLAN_ID,
        COUNT(*)                                                    AS RECHARGES,
        SUM(rc.RECHARGE_AMOUNT_PHP)                                 AS RECHARGE_PHP,
        MODE(rc.RECHARGE_CHANNEL)                                   AS TOP_RECHARGE_CHANNEL
    FROM {{ ref('slv_recharge') }} rc
    JOIN subscriber sb
      ON sb.SUBSCRIPTION_ID = rc.SUBSCRIPTION_ID
    GROUP BY 1, 2, 3
),

churn_agg AS (
    SELECT
        m.RUN_MONTH,
        sb.REGION_NAME,
        sb.PLAN_ID,
        AVG(cs.CHURN_PROBABILITY)                                   AS AVG_CHURN_PROBABILITY,
        COUNT_IF(cs.CHURN_BAND = 'HIGH')                            AS HIGH_RISK_SUBS
    FROM months m
    JOIN {{ ref('slv_churn_score') }} cs
      ON cs.SCORE_DATE <= m.MONTH_END
    JOIN subscriber sb
      ON sb.SUBSCRIPTION_ID = cs.SUBSCRIPTION_ID
    GROUP BY 1, 2, 3
),

segments AS (
    SELECT
        m.RUN_MONTH,
        sb.REGION_NAME,
        sb.PLAN_ID,
        COUNT_IF(sm.SEGMENT_CODE = 'VALUE_HIGH')                    AS HIGH_VALUE_SUBS,
        COUNT_IF(sm.SEGMENT_CODE = 'AT_RISK')                       AS AT_RISK_SUBS
    FROM months m
    JOIN {{ ref('slv_segment_membership') }} sm
      ON m.MONTH_END BETWEEN sm.VALID_FROM AND sm.VALID_TO
    JOIN subscriber sb
      ON sb.SUBSCRIPTION_ID = sm.SUBSCRIPTION_ID
    GROUP BY 1, 2, 3
),

contracts AS (
    SELECT
        m.RUN_MONTH,
        sb.REGION_NAME,
        sb.PLAN_ID,
        COUNT_IF(ct.IS_AUTO_RENEW)                                  AS AUTO_RENEW_CONTRACTS,
        COUNT_IF(ct.END_DATE BETWEEN m.RUN_MONTH AND DATEADD('month', 3, m.MONTH_END)) AS CONTRACTS_ENDING_3M
    FROM months m
    CROSS JOIN {{ ref('slv_contract') }} ct
    JOIN subscriber sb
      ON sb.SUBSCRIPTION_ID = ct.SUBSCRIPTION_ID
    WHERE ct.TERMINATED_ON IS NULL
    GROUP BY 1, 2, 3
),

partner_mix AS (
    SELECT
        sb.REGION_NAME,
        sb.PLAN_ID,
        MODE(b.BRAND_NAME)                                          AS TOP_PARTNER_BRAND,
        AVG(b.REVENUE_SHARE_PCT)                                    AS AVG_PARTNER_SHARE_PCT
    FROM subscriber sb
    JOIN {{ ref('slv_partner_brand') }} b
      ON b.PARTNER_BRAND_ID = sb.PARTNER_BRAND_ID
    GROUP BY sb.REGION_NAME, sb.PLAN_ID
),

/* ---------- 5. collection: invoices of the month vs payments received ---------- */
paid_by_invoice AS (
    SELECT INVOICE_ID, SUM(PAYMENT_AMOUNT_PHP) AS PAID_PHP
    FROM {{ ref('slv_payment') }}
    GROUP BY INVOICE_ID
),

collection AS (
    SELECT
        i.BILLING_MONTH                                             AS RUN_MONTH,
        sb.REGION_NAME,
        sb.PLAN_ID,
        SUM(il.LINE_AMOUNT_PHP)                                     AS INVOICED_PLAN_FEE_PHP,
        SUM(pay.PAID_PHP)                                           AS PAID_PHP
    FROM {{ ref('slv_invoice') }} i
    JOIN months m
      ON m.RUN_MONTH = i.BILLING_MONTH
    JOIN {{ ref('slv_invoice_line') }} il
      ON il.INVOICE_ID = i.INVOICE_ID
     AND il.LINE_TYPE = 'PLAN_FEE'
    JOIN subscriber sb
      ON sb.ACCOUNT_ID = i.ACCOUNT_ID
    LEFT JOIN paid_by_invoice pay
      ON pay.INVOICE_ID = i.INVOICE_ID
    GROUP BY 1, 2, 3
),

/* ---------- 6. combine and rank ---------- */
combined AS (
    SELECT
        rt.RUN_MONTH                                                AS REVENUE_MONTH,
        rt.REGION_NAME,
        rt.PLAN_ID,
        COALESCE(pi.PLAN_NAME, pcu.PLAN_NAME)                       AS PLAN_NAME,
        COALESCE(pi.CONTRACT_TYPE, pcu.CONTRACT_TYPE)               AS CONTRACT_TYPE,
        COALESCE(pi.INTERNET_SERVICE, pcu.INTERNET_SERVICE)         AS INTERNET_SERVICE,
        COALESCE(pi.BASE_PRICE, pcu.BASE_PRICE_PHP)                 AS BASE_PRICE,
        COALESCE(pi.SPEED_MBPS, pcu.SPEED_MBPS)                     AS SPEED_MBPS,
        COALESCE(pi.DATA_CAP_GB, pcu.DATA_CAP_GB)                   AS DATA_CAP_GB,
        pi.SALES_CHANNELS,
        ZEROIFNULL(pf.FEATURES_INCLUDED)                            AS FEATURES_INCLUDED,
        ARRAY_COMPACT(pf.FEATURE_LIST)                              AS FEATURE_LIST,
        ZEROIFNULL(pf.HAS_STREAMING)                                AS HAS_STREAMING,
        ZEROIFNULL(pf.HAS_ROAMING)                                  AS HAS_ROAMING,
        ZEROIFNULL(b.SUBSCRIBERS)                                   AS SUBSCRIBERS,
        ZEROIFNULL(b.ACTIVE_SUBS)                                   AS ACTIVE_SUBS,
        ZEROIFNULL(b.SUSPENDED_SUBS)                                AS SUSPENDED_SUBS,
        ZEROIFNULL(b.PARTNER_SUBS)                                  AS PARTNER_SUBS,
        rt.PAYING_SUBS,
        rt.REVENUE_PHP,
        IFF(rt.LAG_MONTH = DATEADD('month', -1, rt.RUN_MONTH), rt.LAG_REVENUE_PHP, NULL) AS PREV_REVENUE_PHP,
        ZEROIFNULL(rp.REV_PLAN)                                     AS REV_PLAN,
        ZEROIFNULL(rp.REV_VAS)                                      AS REV_VAS,
        ZEROIFNULL(rp.REV_RECHARGE)                                 AS REV_RECHARGE,
        ZEROIFNULL(rp.REV_ROAMING)                                  AS REV_ROAMING,
        ZEROIFNULL(u.DATA_MB)                                       AS DATA_MB,
        ZEROIFNULL(u.VOICE_MIN)                                     AS VOICE_MIN,
        ZEROIFNULL(u.USING_SUBS)                                    AS USING_SUBS,
        ZEROIFNULL(rc.RECHARGES)                                    AS RECHARGES,
        ZEROIFNULL(rc.RECHARGE_PHP)                                 AS RECHARGE_PHP,
        rc.TOP_RECHARGE_CHANNEL,
        ch.AVG_CHURN_PROBABILITY,
        ZEROIFNULL(ch.HIGH_RISK_SUBS)                               AS HIGH_RISK_SUBS,
        ZEROIFNULL(sg.HIGH_VALUE_SUBS)                              AS HIGH_VALUE_SUBS,
        ZEROIFNULL(sg.AT_RISK_SUBS)                                 AS AT_RISK_SUBS,
        ZEROIFNULL(ct.AUTO_RENEW_CONTRACTS)                         AS AUTO_RENEW_CONTRACTS,
        ZEROIFNULL(ct.CONTRACTS_ENDING_3M)                          AS CONTRACTS_ENDING_3M,
        pm.TOP_PARTNER_BRAND,
        pm.AVG_PARTNER_SHARE_PCT,
        ZEROIFNULL(co.INVOICED_PLAN_FEE_PHP)                        AS INVOICED_PLAN_FEE_PHP,
        ZEROIFNULL(co.PAID_PHP)                                     AS PAID_PHP
    FROM revenue_totals rt
    LEFT JOIN revenue_pivot rp   ON rp.RUN_MONTH = rt.RUN_MONTH AND rp.REGION_NAME = rt.REGION_NAME AND rp.PLAN_ID = rt.PLAN_ID
    LEFT JOIN plan_info pi       ON pi.RUN_MONTH = rt.RUN_MONTH AND pi.PLAN_ID = rt.PLAN_ID
    LEFT JOIN plan_current pcu   ON pcu.PLAN_ID = rt.PLAN_ID
    LEFT JOIN plan_features pf   ON pf.RUN_MONTH = rt.RUN_MONTH AND pf.PLAN_ID = rt.PLAN_ID
    LEFT JOIN base b             ON b.REGION_NAME = rt.REGION_NAME AND b.PLAN_ID = rt.PLAN_ID
    LEFT JOIN usage u            ON u.RUN_MONTH = rt.RUN_MONTH AND u.REGION_NAME = rt.REGION_NAME AND u.PLAN_ID = rt.PLAN_ID
    LEFT JOIN recharges rc       ON rc.RUN_MONTH = rt.RUN_MONTH AND rc.REGION_NAME = rt.REGION_NAME AND rc.PLAN_ID = rt.PLAN_ID
    LEFT JOIN churn_agg ch       ON ch.RUN_MONTH = rt.RUN_MONTH AND ch.REGION_NAME = rt.REGION_NAME AND ch.PLAN_ID = rt.PLAN_ID
    LEFT JOIN segments sg        ON sg.RUN_MONTH = rt.RUN_MONTH AND sg.REGION_NAME = rt.REGION_NAME AND sg.PLAN_ID = rt.PLAN_ID
    LEFT JOIN contracts ct       ON ct.RUN_MONTH = rt.RUN_MONTH AND ct.REGION_NAME = rt.REGION_NAME AND ct.PLAN_ID = rt.PLAN_ID
    LEFT JOIN partner_mix pm     ON pm.REGION_NAME = rt.REGION_NAME AND pm.PLAN_ID = rt.PLAN_ID
    LEFT JOIN collection co      ON co.RUN_MONTH = rt.RUN_MONTH AND co.REGION_NAME = rt.REGION_NAME AND co.PLAN_ID = rt.PLAN_ID
    WHERE rt.REVENUE_PHP > 0
),

ranked AS (
    SELECT
        c.*,
        ROUND(RATIO_TO_REPORT(c.REVENUE_PHP) OVER (PARTITION BY c.REVENUE_MONTH, c.REGION_NAME), 6) AS SHARE_OF_REGION,
        ROUND(RATIO_TO_REPORT(c.REVENUE_PHP) OVER (PARTITION BY c.REVENUE_MONTH), 8)                AS SHARE_OF_COUNTRY,
        RANK()       OVER (PARTITION BY c.REVENUE_MONTH, c.REGION_NAME ORDER BY c.REVENUE_PHP DESC, c.PLAN_ID) AS RANK_IN_REGION,
        DENSE_RANK() OVER (PARTITION BY c.REVENUE_MONTH ORDER BY c.REVENUE_PHP DESC)                AS DENSE_RANK_NATIONAL,
        ROUND(CUME_DIST() OVER (PARTITION BY c.REVENUE_MONTH, c.REGION_NAME ORDER BY c.REVENUE_PHP DESC), 4) AS CUME_DIST_IN_REGION,
        NTILE(10)    OVER (PARTITION BY c.REVENUE_MONTH, c.REGION_NAME ORDER BY c.REVENUE_PHP DESC) AS DECILE_IN_REGION,
        SUM(c.REVENUE_PHP) OVER (PARTITION BY c.REVENUE_MONTH, c.REGION_NAME ORDER BY c.REVENUE_PHP DESC, c.PLAN_ID
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)                  AS RUNNING_REVENUE_IN_REGION,
        LAG(c.PLAN_NAME) OVER (PARTITION BY c.REVENUE_MONTH, c.REGION_NAME ORDER BY c.REVENUE_PHP DESC, c.PLAN_ID) AS NEXT_HIGHER_PLAN,
        c.REVENUE_PHP - LAG(c.REVENUE_PHP) OVER (PARTITION BY c.REVENUE_MONTH, c.REGION_NAME ORDER BY c.REVENUE_PHP DESC, c.PLAN_ID) AS GAP_TO_NEXT_HIGHER_PHP,
        MEDIAN(c.REVENUE_PHP) OVER (PARTITION BY c.REVENUE_MONTH, c.REGION_NAME)                    AS REGION_MEDIAN_PLAN_REVENUE
    FROM combined c
)

SELECT
    r.REVENUE_MONTH,
    r.REGION_NAME,
    r.PLAN_ID,
    r.PLAN_NAME,
    r.CONTRACT_TYPE,
    r.INTERNET_SERVICE,
    r.BASE_PRICE,
    r.SPEED_MBPS,
    r.DATA_CAP_GB,
    r.SALES_CHANNELS,
    r.FEATURES_INCLUDED,
    r.FEATURE_LIST,
    r.HAS_STREAMING,
    r.HAS_ROAMING,
    r.SUBSCRIBERS,
    r.ACTIVE_SUBS,
    r.SUSPENDED_SUBS,
    r.PARTNER_SUBS,
    r.PAYING_SUBS,
    ROUND(r.REVENUE_PHP, 2)                                         AS REVENUE_PHP,
    ROUND(r.PREV_REVENUE_PHP, 2)                                    AS PREV_REVENUE_PHP,
    ROUND(DIV0(r.REVENUE_PHP - r.PREV_REVENUE_PHP, r.PREV_REVENUE_PHP) * 100, 2) AS MOM_GROWTH_PCT,
    ROUND(r.REV_PLAN, 2)                                            AS REV_PLAN,
    ROUND(r.REV_VAS, 2)                                             AS REV_VAS,
    ROUND(r.REV_RECHARGE, 2)                                        AS REV_RECHARGE,
    ROUND(r.REV_ROAMING, 2)                                         AS REV_ROAMING,
    ROUND(DIV0(r.REV_VAS + r.REV_ROAMING, r.REVENUE_PHP), 4)        AS NON_PLAN_SHARE,
    r.SHARE_OF_REGION,
    r.SHARE_OF_COUNTRY,
    r.RANK_IN_REGION,
    r.DENSE_RANK_NATIONAL,
    r.CUME_DIST_IN_REGION,
    r.DECILE_IN_REGION,
    ROUND(r.RUNNING_REVENUE_IN_REGION, 2)                           AS RUNNING_REVENUE_IN_REGION,
    r.NEXT_HIGHER_PLAN,
    ROUND(r.GAP_TO_NEXT_HIGHER_PHP, 2)                              AS GAP_TO_NEXT_HIGHER_PHP,
    ROUND(r.REGION_MEDIAN_PLAN_REVENUE, 2)                          AS REGION_MEDIAN_PLAN_REVENUE,
    ROUND(DIV0(r.REVENUE_PHP, r.PAYING_SUBS), 2)                    AS ARPU_PHP,
    ROUND(DIV0(r.DATA_MB, r.USING_SUBS), 2)                         AS MB_PER_USER,
    ROUND(DIV0(r.VOICE_MIN, r.USING_SUBS), 2)                       AS MIN_PER_USER,
    ROUND(DIV0(r.REVENUE_PHP, r.DATA_MB / 1024), 4)                 AS PHP_PER_GB,
    r.RECHARGES,
    ROUND(r.RECHARGE_PHP, 2)                                        AS RECHARGE_PHP,
    r.TOP_RECHARGE_CHANNEL,
    ROUND(r.AVG_CHURN_PROBABILITY, 4)                               AS AVG_CHURN_PROBABILITY,
    r.HIGH_RISK_SUBS,
    ROUND(r.REVENUE_PHP * DIV0(r.HIGH_RISK_SUBS, r.SUBSCRIBERS), 2) AS REVENUE_AT_RISK_PHP,
    r.HIGH_VALUE_SUBS,
    r.AT_RISK_SUBS,
    r.AUTO_RENEW_CONTRACTS,
    r.CONTRACTS_ENDING_3M,
    r.TOP_PARTNER_BRAND,
    r.AVG_PARTNER_SHARE_PCT,
    ROUND(r.INVOICED_PLAN_FEE_PHP, 2)                               AS INVOICED_PLAN_FEE_PHP,
    ROUND(r.PAID_PHP, 2)                                            AS PAID_PHP,
    ROUND(DIV0(r.PAID_PHP, r.INVOICED_PLAN_FEE_PHP), 4)             AS COLLECTION_RATE,
    CASE
        WHEN r.RANK_IN_REGION = 1                                   THEN 'LEADER'
        WHEN r.SHARE_OF_REGION >= 0.10                              THEN 'CORE'
        WHEN DIV0(r.REVENUE_PHP - r.PREV_REVENUE_PHP, r.PREV_REVENUE_PHP) > 0.10 THEN 'GROWING'
        ELSE 'TAIL'
    END                                                             AS PLAN_ROLE,
    CURRENT_TIMESTAMP()                                             AS LOADED_AT
FROM ranked r
QUALIFY r.RANK_IN_REGION <= 10
