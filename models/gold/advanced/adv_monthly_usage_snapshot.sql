{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['snapshot_month'],
    on_schema_change = 'append_new_columns',
    transient = false,
    cluster_by = ['snapshot_month', 'plan_id'],
    post_hook = [
      "delete from {{ this }} where snapshot_month < dateadd('month', -{{ var('adv_snapshot_retention_months', 12) }}, (select max(snapshot_month) from {{ this }}))"
    ],
    tags = ['gold', 'advanced', 'h12', 'usage', 'snapshot'],
    query_tag = 'dbt_telco_adv_h12'
  )
}}
{#-
  adv_monthly_usage_snapshot  -  Monthly subscriber usage snapshot
  ------------------------------------------------------------------------------------------
  Ported from x_h12_dynamic_monthly_snapshot (PIPE_X.SP_X_H12_DYNAMIC_MONTHLY_SNAPSHOT).

  The procedure used EXECUTE IMMEDIATE to create one table per month (H12_USAGE_YYYYMM),
  a registry table and a cursor dropping snapshots older than 12 months. Here it is one
  table keyed by snapshot_month; each run replaces its months (delete+insert) and a
  post-hook enforces the retention (var adv_snapshot_retention_months, default 12).

  Per subscriber and month: usage by weekday / weekend, peak day, inactive streaks, data-cap
  use, month-over-month change, revenue by source, recharges, roaming, tickets, contract
  and segment at month end, churn score, and usage deciles.

  Grain     : snapshot_month x subscription_id (lines with usage in the month).
  Window    : closed months (<= last BSS status month) from var('history_start_date');
              incremental restates the last loaded month and later. The first month in the
              window has no previous month in gold usage, so its MoM columns are NULL/0.
  Changed vs original: dynamic per-month tables -> one incremental table; one RUN_MONTH ->
              all months (deciles / shares partition by month); usage from gold
              fct_subscriber_usage_daily (CDR-derived) instead of the legacy USAGE_DAILY_AGG.
  Snowflake : CONDITIONAL_CHANGE_EVENT, GENERATOR, DAYOFWEEKISO, MAX_BY, ARRAY_AGG WITHIN
              GROUP, LISTAGG DISTINCT, NTILE, RATIO_TO_REPORT, MEDIAN, STDDEV_SAMP,
              TRY_TO_NUMBER, OBJECT_CONSTRUCT, DIV0, ZEROIFNULL, IFF, QUALIFY, post-hook DML.
  Sources (13): fct_subscriber_usage_daily, slv_subscription, slv_account, slv_customer,
              slv_plan, ref.plan_feature (bronze, effective dated), slv_daily_revenue,
              slv_recharge, slv_roaming_usage, slv_ticket, slv_contract,
              slv_segment_membership, slv_churn_score.
-#}

WITH bounds AS (
    SELECT
        {{ f3_month_lower_bound('snapshot_month') }}                AS WINDOW_START,
        {{ f3_as_of_month() }}                                      AS LAST_MONTH
),

months AS (
    SELECT
        RUN_MONTH,
        LAST_DAY(RUN_MONTH)                                         AS MONTH_END,
        DATEADD('month', 1, RUN_MONTH)                              AS NEXT_MONTH,
        DATEADD('month', -1, RUN_MONTH)                             AS PREV_MONTH,
        DAY(LAST_DAY(RUN_MONTH))                                    AS DAYS_IN_MONTH
    FROM (
        SELECT DISTINCT DATE_TRUNC('month', u.EVENT_DATE)::DATE     AS RUN_MONTH
        FROM {{ ref('fct_subscriber_usage_daily') }} u
        CROSS JOIN bounds b
        WHERE u.EVENT_DATE >= b.WINDOW_START
          AND u.EVENT_DATE <  DATEADD('month', 1, b.LAST_MONTH)
    )
),

/* ---------- 1. daily usage of each month and the month before ---------- */
daily AS (
    SELECT
        DATE_TRUNC('month', u.EVENT_DATE)::DATE                     AS USAGE_MONTH,
        u.SUBSCRIPTION_ID,
        u.EVENT_DATE                                                AS USAGE_DATE,
        u.DATA_MB,
        u.VOICE_MIN,
        u.SMS_COUNT,
        u.DATA_SESSIONS                                             AS SESSION_COUNT,
        IFF(DAYOFWEEKISO(u.EVENT_DATE) >= 6, TRUE, FALSE)           AS IS_WEEKEND
    FROM {{ ref('fct_subscriber_usage_daily') }} u
    WHERE u.EVENT_DATE >= (SELECT DATEADD('month', -1, MIN(RUN_MONTH)) FROM months)
      AND u.EVENT_DATE <  (SELECT MAX(NEXT_MONTH) FROM months)
),

usage_cur AS (
    SELECT
        d.USAGE_MONTH                                               AS RUN_MONTH,
        d.SUBSCRIPTION_ID,
        SUM(d.DATA_MB)                                              AS DATA_MB,
        SUM(d.VOICE_MIN)                                            AS VOICE_MIN,
        SUM(d.SMS_COUNT)                                            AS SMS,
        SUM(d.SESSION_COUNT)                                        AS SESSIONS,
        COUNT(*)                                                    AS ACTIVE_DAYS,
        SUM(IFF(d.IS_WEEKEND, d.DATA_MB, 0))                        AS WEEKEND_DATA_MB,
        SUM(IFF(NOT d.IS_WEEKEND, d.DATA_MB, 0))                    AS WEEKDAY_DATA_MB,
        COUNT_IF(d.IS_WEEKEND)                                      AS WEEKEND_ACTIVE_DAYS,
        MAX(d.DATA_MB)                                              AS PEAK_DAY_MB,
        MAX_BY(d.USAGE_DATE, d.DATA_MB)                             AS PEAK_DAY,
        MEDIAN(d.DATA_MB)                                           AS MEDIAN_DAY_MB,
        STDDEV_SAMP(d.DATA_MB)                                      AS STDDEV_DAY_MB,
        MIN(d.USAGE_DATE)                                           AS FIRST_ACTIVE_DAY,
        MAX(d.USAGE_DATE)                                           AS LAST_ACTIVE_DAY
    FROM daily d
    JOIN months m
      ON m.RUN_MONTH = d.USAGE_MONTH
    GROUP BY d.USAGE_MONTH, d.SUBSCRIPTION_ID
),

usage_prev AS (
    SELECT
        DATEADD('month', 1, d.USAGE_MONTH)::DATE                    AS RUN_MONTH,
        d.SUBSCRIPTION_ID,
        SUM(d.DATA_MB)                                              AS PREV_DATA_MB,
        SUM(d.VOICE_MIN)                                            AS PREV_VOICE_MIN,
        COUNT(*)                                                    AS PREV_ACTIVE_DAYS
    FROM daily d
    GROUP BY 1, 2
),

/* ---------- 2. inactive streaks: a calendar of each month per subscriber ---------- */
calendar AS (
    SELECT
        m.RUN_MONTH,
        DATEADD('day', g.N, m.RUN_MONTH)                            AS CAL_DATE
    FROM months m
    CROSS JOIN (
        SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1              AS N
        FROM TABLE(GENERATOR(ROWCOUNT => 31))
    ) g
    WHERE g.N < m.DAYS_IN_MONTH
),

sub_calendar AS (
    SELECT
        uc.RUN_MONTH,
        uc.SUBSCRIPTION_ID,
        c.CAL_DATE,
        IFF(d.SUBSCRIPTION_ID IS NULL, 0, 1)                        AS ACTIVE
    FROM usage_cur uc
    JOIN calendar c
      ON c.RUN_MONTH = uc.RUN_MONTH
    LEFT JOIN daily d
           ON d.SUBSCRIPTION_ID = uc.SUBSCRIPTION_ID
          AND d.USAGE_DATE = c.CAL_DATE
),

streaks AS (
    SELECT
        RUN_MONTH,
        SUBSCRIPTION_ID,
        ACTIVE,
        CONDITIONAL_CHANGE_EVENT(ACTIVE) OVER (PARTITION BY RUN_MONTH, SUBSCRIPTION_ID ORDER BY CAL_DATE) AS RUN_NO,
        CAL_DATE
    FROM sub_calendar
),

streak_lengths AS (
    SELECT
        RUN_MONTH,
        SUBSCRIPTION_ID,
        MAX(IFF(ACTIVE = 0, RUN_LEN, 0))                            AS LONGEST_INACTIVE_STREAK,
        MAX(IFF(ACTIVE = 1, RUN_LEN, 0))                            AS LONGEST_ACTIVE_STREAK,
        COUNT_IF(ACTIVE = 0)                                        AS INACTIVE_RUNS
    FROM (
        SELECT RUN_MONTH, SUBSCRIPTION_ID, ACTIVE, RUN_NO, COUNT(*) AS RUN_LEN
        FROM streaks
        GROUP BY RUN_MONTH, SUBSCRIPTION_ID, ACTIVE, RUN_NO
    )
    GROUP BY RUN_MONTH, SUBSCRIPTION_ID
),

/* ---------- 3. subscriber, plan and cap (per month) ---------- */
subscriber AS (
    SELECT
        uc.RUN_MONTH,
        s.SUBSCRIPTION_ID,
        s.ACCOUNT_ID,
        s.PLAN_ID,
        s.STATUS,
        s.TENURE_MONTHS,
        s.MONTHLY_CHARGES_PHP                                       AS MONTHLY_CHARGES,
        s.INTERNET_SERVICE,
        a.PAYMENT_METHOD,
        c.PREFERRED_LANGUAGE,
        DATEDIFF('year', c.DATE_OF_BIRTH, LAST_DAY(uc.RUN_MONTH))   AS AGE,
        pc.PLAN_NAME,
        pc.CONTRACT_TYPE                                            AS PLAN_CONTRACT_TYPE,
        pc.BASE_PRICE_PHP                                           AS BASE_PRICE,
        COALESCE(TRY_TO_NUMBER(pf.FEATURE_VALUE), pc.DATA_CAP_GB)   AS DATA_CAP_GB
    FROM usage_cur uc
    JOIN {{ ref('slv_subscription') }} s
      ON s.SUBSCRIPTION_ID = uc.SUBSCRIPTION_ID
    LEFT JOIN {{ ref('slv_account') }} a
           ON a.ACCOUNT_ID = s.ACCOUNT_ID
    LEFT JOIN {{ ref('slv_customer') }} c
           ON c.CUSTOMER_ID = a.CUSTOMER_ID
    LEFT JOIN {{ ref('slv_plan') }} pc
           ON pc.PLAN_ID = s.PLAN_ID
    LEFT JOIN {{ source('ref', 'plan_feature') }} pf
           ON pf.PLAN_ID = s.PLAN_ID
          AND pf.FEATURE_CODE = 'DATA_CAP'
          AND LAST_DAY(uc.RUN_MONTH) BETWEEN pf.EFFECTIVE_FROM AND COALESCE(pf.EFFECTIVE_TO, '9999-12-31'::DATE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uc.RUN_MONTH, s.SUBSCRIPTION_ID ORDER BY pf.EFFECTIVE_FROM DESC NULLS LAST) = 1
),

/* ---------- 4. money and service ---------- */
revenue AS (
    SELECT
        DATE_TRUNC('month', r.BUSINESS_DATE)::DATE                  AS RUN_MONTH,
        r.SUBSCRIPTION_ID,
        SUM(r.REVENUE_PHP)                                          AS REVENUE_PHP,
        SUM(IFF(r.REVENUE_SOURCE = 'PLAN', r.REVENUE_PHP, 0))       AS REV_PLAN,
        SUM(IFF(r.REVENUE_SOURCE = 'VAS', r.REVENUE_PHP, 0))        AS REV_VAS,
        SUM(IFF(r.REVENUE_SOURCE = 'RECHARGE', r.REVENUE_PHP, 0))   AS REV_RECHARGE,
        SUM(IFF(r.REVENUE_SOURCE = 'ROAMING', r.REVENUE_PHP, 0))    AS REV_ROAMING
    FROM {{ ref('slv_daily_revenue') }} r
    JOIN months m
      ON r.BUSINESS_DATE >= m.RUN_MONTH
     AND r.BUSINESS_DATE <  m.NEXT_MONTH
    GROUP BY 1, 2
),

recharges AS (
    SELECT
        DATE_TRUNC('month', rc.EVENT_DATE)::DATE                    AS RUN_MONTH,
        rc.SUBSCRIPTION_ID,
        COUNT(*)                                                    AS RECHARGES,
        SUM(rc.RECHARGE_AMOUNT_PHP)                                 AS RECHARGE_PHP,
        ARRAY_AGG(DISTINCT rc.PACK_CODE) WITHIN GROUP (ORDER BY rc.PACK_CODE) AS PACKS,
        MAX_BY(rc.RECHARGE_CHANNEL, rc.RECHARGED_AT_LOCAL)          AS LAST_CHANNEL
    FROM {{ ref('slv_recharge') }} rc
    JOIN months m
      ON rc.EVENT_DATE >= m.RUN_MONTH
     AND rc.EVENT_DATE <  m.NEXT_MONTH
    GROUP BY 1, 2
),

roaming AS (
    SELECT
        DATE_TRUNC('month', ru.EVENT_DATE)::DATE                    AS RUN_MONTH,
        ru.SUBSCRIPTION_ID,
        COUNT(DISTINCT ru.VISITED_COUNTRY)                          AS COUNTRIES_VISITED,
        SUM(ru.DATA_MB)                                             AS ROAMING_MB,
        SUM(ru.CHARGE_PHP_BILLED)                                   AS ROAMING_PHP
    FROM {{ ref('slv_roaming_usage') }} ru
    JOIN months m
      ON ru.EVENT_DATE >= m.RUN_MONTH
     AND ru.EVENT_DATE <  m.NEXT_MONTH
    GROUP BY 1, 2
),

tickets AS (
    SELECT
        DATE_TRUNC('month', t.OPENED_DATE)::DATE                    AS RUN_MONTH,
        t.SUBSCRIPTION_ID,
        COUNT(*)                                                    AS TICKETS,
        COUNT_IF(t.CATEGORY IN ('NO_INTERNET', 'SLOW_SPEED'))       AS NETWORK_TICKETS
    FROM {{ ref('slv_ticket') }} t
    JOIN months m
      ON t.OPENED_DATE >= m.RUN_MONTH
     AND t.OPENED_DATE <  m.NEXT_MONTH
    GROUP BY 1, 2
),

contract_end AS (
    SELECT
        m.RUN_MONTH,
        ct.SUBSCRIPTION_ID,
        ct.CONTRACT_TYPE,
        ct.END_DATE,
        ct.IS_AUTO_RENEW                                            AS AUTO_RENEW
    FROM months m
    JOIN {{ ref('slv_contract') }} ct
      ON ct.START_DATE <= m.MONTH_END
     AND (ct.TERMINATED_ON IS NULL OR ct.TERMINATED_ON > m.MONTH_END)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY m.RUN_MONTH, ct.SUBSCRIPTION_ID ORDER BY ct.START_DATE DESC) = 1
),

segments AS (
    SELECT
        m.RUN_MONTH,
        sm.SUBSCRIPTION_ID,
        LISTAGG(DISTINCT sm.SEGMENT_CODE, '|') WITHIN GROUP (ORDER BY sm.SEGMENT_CODE) AS SEGMENTS
    FROM months m
    JOIN {{ ref('slv_segment_membership') }} sm
      ON m.MONTH_END BETWEEN sm.VALID_FROM AND sm.VALID_TO
    GROUP BY 1, 2
),

churn AS (
    SELECT
        m.RUN_MONTH,
        cs.SUBSCRIPTION_ID,
        cs.CHURN_PROBABILITY
    FROM months m
    JOIN {{ ref('slv_churn_score') }} cs
      ON cs.SCORE_DATE <= m.MONTH_END
),

/* ---------- 5. assemble ---------- */
assembled AS (
    SELECT
        uc.RUN_MONTH                                                AS SNAPSHOT_MONTH,
        uc.SUBSCRIPTION_ID,
        sb.ACCOUNT_ID,
        sb.PLAN_ID,
        sb.PLAN_NAME,
        sb.PLAN_CONTRACT_TYPE,
        sb.BASE_PRICE,
        sb.STATUS,
        sb.TENURE_MONTHS,
        sb.MONTHLY_CHARGES,
        sb.INTERNET_SERVICE,
        sb.PAYMENT_METHOD,
        sb.PREFERRED_LANGUAGE,
        sb.AGE,
        ROUND(uc.DATA_MB, 3)                                        AS DATA_MB,
        ROUND(uc.VOICE_MIN, 2)                                      AS VOICE_MIN,
        uc.SMS,
        uc.SESSIONS,
        uc.ACTIVE_DAYS,
        ROUND(DIV0(uc.ACTIVE_DAYS, m.DAYS_IN_MONTH), 4)             AS ACTIVE_DAY_RATIO,
        ROUND(uc.WEEKEND_DATA_MB, 3)                                AS WEEKEND_DATA_MB,
        ROUND(uc.WEEKDAY_DATA_MB, 3)                                AS WEEKDAY_DATA_MB,
        ROUND(DIV0(uc.WEEKEND_DATA_MB, uc.DATA_MB), 4)              AS WEEKEND_SHARE,
        uc.PEAK_DAY,
        ROUND(uc.PEAK_DAY_MB, 3)                                    AS PEAK_DAY_MB,
        ROUND(uc.MEDIAN_DAY_MB, 3)                                  AS MEDIAN_DAY_MB,
        ROUND(DIV0(uc.STDDEV_DAY_MB, DIV0(uc.DATA_MB, uc.ACTIVE_DAYS)), 4) AS USAGE_VOLATILITY,
        uc.FIRST_ACTIVE_DAY,
        uc.LAST_ACTIVE_DAY,
        ZEROIFNULL(sl.LONGEST_INACTIVE_STREAK)                      AS LONGEST_INACTIVE_STREAK,
        ZEROIFNULL(sl.LONGEST_ACTIVE_STREAK)                        AS LONGEST_ACTIVE_STREAK,
        sb.DATA_CAP_GB,
        ROUND(DIV0(uc.DATA_MB / 1024, sb.DATA_CAP_GB), 4)           AS CAP_UTILISATION,
        IFF(sb.DATA_CAP_GB IS NOT NULL AND uc.DATA_MB / 1024 > sb.DATA_CAP_GB, TRUE, FALSE) AS OVER_CAP,
        ROUND(ZEROIFNULL(up.PREV_DATA_MB), 3)                       AS PREV_DATA_MB,
        ROUND(DIV0(uc.DATA_MB - up.PREV_DATA_MB, up.PREV_DATA_MB) * 100, 2) AS DATA_MOM_PCT,
        ROUND(DIV0(uc.VOICE_MIN - up.PREV_VOICE_MIN, up.PREV_VOICE_MIN) * 100, 2) AS VOICE_MOM_PCT,
        ZEROIFNULL(up.PREV_ACTIVE_DAYS)                             AS PREV_ACTIVE_DAYS,
        ROUND(ZEROIFNULL(rv.REVENUE_PHP), 2)                        AS REVENUE_PHP,
        ROUND(ZEROIFNULL(rv.REV_PLAN), 2)                           AS REV_PLAN,
        ROUND(ZEROIFNULL(rv.REV_VAS), 2)                            AS REV_VAS,
        ROUND(ZEROIFNULL(rv.REV_RECHARGE), 2)                       AS REV_RECHARGE,
        ROUND(ZEROIFNULL(rv.REV_ROAMING), 2)                        AS REV_ROAMING,
        ROUND(DIV0(rv.REVENUE_PHP, uc.DATA_MB / 1024), 4)           AS REVENUE_PER_GB,
        ZEROIFNULL(rc.RECHARGES)                                    AS RECHARGES,
        ROUND(ZEROIFNULL(rc.RECHARGE_PHP), 2)                       AS RECHARGE_PHP,
        rc.PACKS                                                    AS RECHARGE_PACKS,
        rc.LAST_CHANNEL                                             AS LAST_RECHARGE_CHANNEL,
        ZEROIFNULL(ro.COUNTRIES_VISITED)                            AS COUNTRIES_VISITED,
        ROUND(ZEROIFNULL(ro.ROAMING_MB), 3)                         AS ROAMING_MB,
        ROUND(ZEROIFNULL(ro.ROAMING_PHP), 2)                        AS ROAMING_PHP,
        ZEROIFNULL(tk.TICKETS)                                      AS TICKETS,
        ZEROIFNULL(tk.NETWORK_TICKETS)                              AS NETWORK_TICKETS,
        ce.CONTRACT_TYPE,
        ce.END_DATE                                                 AS CONTRACT_END_DATE,
        ce.AUTO_RENEW,
        DATEDIFF('month', m.MONTH_END, ce.END_DATE)                 AS CONTRACT_MONTHS_LEFT,
        sg.SEGMENTS,
        ch.CHURN_PROBABILITY
    FROM usage_cur uc
    JOIN months m                ON m.RUN_MONTH = uc.RUN_MONTH
    LEFT JOIN usage_prev up      ON up.SUBSCRIPTION_ID = uc.SUBSCRIPTION_ID AND up.RUN_MONTH = uc.RUN_MONTH
    LEFT JOIN streak_lengths sl  ON sl.SUBSCRIPTION_ID = uc.SUBSCRIPTION_ID AND sl.RUN_MONTH = uc.RUN_MONTH
    LEFT JOIN subscriber sb      ON sb.SUBSCRIPTION_ID = uc.SUBSCRIPTION_ID AND sb.RUN_MONTH = uc.RUN_MONTH
    LEFT JOIN revenue rv         ON rv.SUBSCRIPTION_ID = uc.SUBSCRIPTION_ID AND rv.RUN_MONTH = uc.RUN_MONTH
    LEFT JOIN recharges rc       ON rc.SUBSCRIPTION_ID = uc.SUBSCRIPTION_ID AND rc.RUN_MONTH = uc.RUN_MONTH
    LEFT JOIN roaming ro         ON ro.SUBSCRIPTION_ID = uc.SUBSCRIPTION_ID AND ro.RUN_MONTH = uc.RUN_MONTH
    LEFT JOIN tickets tk         ON tk.SUBSCRIPTION_ID = uc.SUBSCRIPTION_ID AND tk.RUN_MONTH = uc.RUN_MONTH
    LEFT JOIN contract_end ce    ON ce.SUBSCRIPTION_ID = uc.SUBSCRIPTION_ID AND ce.RUN_MONTH = uc.RUN_MONTH
    LEFT JOIN segments sg        ON sg.SUBSCRIPTION_ID = uc.SUBSCRIPTION_ID AND sg.RUN_MONTH = uc.RUN_MONTH
    LEFT JOIN churn ch           ON ch.SUBSCRIPTION_ID = uc.SUBSCRIPTION_ID AND ch.RUN_MONTH = uc.RUN_MONTH
)

SELECT
    a.*,
    NTILE(10) OVER (PARTITION BY a.SNAPSHOT_MONTH ORDER BY a.DATA_MB, a.SUBSCRIPTION_ID)     AS DATA_DECILE,
    NTILE(10) OVER (PARTITION BY a.SNAPSHOT_MONTH ORDER BY a.REVENUE_PHP, a.SUBSCRIPTION_ID) AS REVENUE_DECILE,
    NTILE(4)  OVER (PARTITION BY a.SNAPSHOT_MONTH, a.PLAN_ID ORDER BY a.DATA_MB, a.SUBSCRIPTION_ID) AS DATA_QUARTILE_IN_PLAN,
    ROUND(RATIO_TO_REPORT(a.REVENUE_PHP) OVER (PARTITION BY a.SNAPSHOT_MONTH, a.PLAN_ID), 8) AS REVENUE_SHARE_IN_PLAN,
    CASE
        WHEN a.ACTIVE_DAYS <= 3                                     THEN 'DORMANT'
        WHEN a.LONGEST_INACTIVE_STREAK >= 10                        THEN 'LAPSING'
        WHEN a.OVER_CAP                                             THEN 'CAP_BREAKER'
        WHEN a.DATA_MOM_PCT <= -40                                  THEN 'DECLINING'
        WHEN a.DATA_MOM_PCT >= 40                                   THEN 'GROWING'
        ELSE 'STEADY'
    END                                                             AS USAGE_STATE,
    OBJECT_CONSTRUCT(
        'data_mb', a.DATA_MB,
        'voice_min', a.VOICE_MIN,
        'sms', a.SMS,
        'revenue_php', a.REVENUE_PHP,
        'tickets', a.TICKETS
    )                                                               AS SUMMARY,
    CURRENT_TIMESTAMP()                                             AS LOADED_AT
FROM assembled a
