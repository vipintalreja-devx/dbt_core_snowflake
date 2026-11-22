{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['journey_month'],
    on_schema_change = 'sync_all_columns',
    transient = false,
    cluster_by = ['journey_month', 'journey_type', 'home_region_name'],
    tags = ['gold', 'advanced', 'h05', 'journey', 'churn'],
    query_tag = 'dbt_telco_adv_h05',
    post_hook = [
      "delete from {{ this }} t using (select journey_key, max(loaded_at) as latest from {{ this }} group by 1 having count(*) > 1) d where t.journey_key = d.journey_key and t.loaded_at < d.latest"
    ]
  )
}}
{#-
  adv_subscriber_journey  -  Subscriber status journeys
  ------------------------------------------------------------------------------------------
  Ported from x_h05_subscriber_journey_match (PIPE_X.SP_X_H05_SUBSCRIBER_JOURNEY_MATCH).

  Finds subscriber status journeys that END in each month of the reporting window and
  enriches every journey with what happened around it: revenue before and after, usage
  trend, tickets, NPS, churn score, segment, recharges, dunning and outages.

  Journeys (MATCH_RECOGNIZE over the cleaned status history):
    WIN_BACK_AFTER_CHURN    ACTIVE -> (SUSPENDED)* -> CHURNED -> ACTIVE
    LOST_AFTER_SUSPEND      ACTIVE -> SUSPENDED+   -> CHURNED
    RECOVERED_FROM_SUSPEND  ACTIVE -> SUSPENDED+   -> ACTIVE
    DIRECT_CHURN            ACTIVE -> CHURNED

  A journey can change its end (e.g. DIRECT_CHURN in July becomes WIN_BACK_AFTER_CHURN in
  November when the line is won back). Months are replaced with delete+insert, so the post-hook
  keeps only the latest version of each journey_key.
  Grain     : one row per journey (subscription_id, journey_start); journey_month = month
              the journey ended.
  Window    : every month from var('history_start_date') up to the last BSS status month;
              incremental runs restate the last loaded month and everything after it.
              Status history is read 18 months back (silver keeps full history); revenue and
              usage come from silver/gold, which only hold the reporting window, so the
              "90 days before" measures are partial for journeys starting before it.
  Changed vs original: single RUN_MONTH param -> all months; journey key no longer uses
              MATCH_NUMBER (it depends on the scanned range) but (subscription, start);
              timestamps compared in Manila local time via silver *_local columns.
  Snowflake : MATCH_RECOGNIZE, CONDITIONAL_CHANGE_EVENT, QUALIFY, IFF, DIV0, ZEROIFNULL,
              REGR_SLOPE, MAX_BY, MODE, ARRAY_AGG ... WITHIN GROUP, LISTAGG DISTINCT,
              BOOLOR_AGG, OBJECT_CONSTRUCT_KEEP_NULL, RATIO_TO_REPORT, EQUAL_NULL, HASH.
  Sources (19): slv_subscription_status_history, slv_subscription, slv_account,
              slv_customer, slv_plan, slv_contract, slv_cell_site, slv_region,
              slv_partner_subscriber_map, slv_partner_brand, slv_daily_revenue,
              fct_subscriber_usage_daily, slv_ticket, slv_nps_survey, slv_churn_score,
              slv_segment_membership, slv_recharge, slv_dunning_event, slv_outage_impact,
              slv_outage_incident.
-#}

WITH params AS (
    SELECT
        {{ f3_month_lower_bound('journey_month') }}                 AS WINDOW_START,
        {{ f3_as_of_month() }}                                      AS LAST_MONTH,
        DATEADD('month', 1, {{ f3_as_of_month() }})                 AS WINDOW_END,
        DATEADD('month', -18, {{ f3_month_lower_bound('journey_month') }}) AS HISTORY_START
),

/* ---------- 1. status history: normalise, collapse repeated statuses ---------- */
status_raw AS (
    SELECT
        h.SUBSCRIPTION_ID,
        h.STATUS,
        h.VALID_FROM_LOCAL                                          AS VALID_FROM,
        h.VALID_TO_LOCAL                                            AS VALID_TO
    FROM {{ ref('slv_subscription_status_history') }} h
    CROSS JOIN params p
    WHERE h.VALID_FROM_LOCAL >= p.HISTORY_START
      AND h.VALID_FROM_LOCAL <  p.WINDOW_END
      AND h.STATUS IS NOT NULL
),

status_runs AS (
    SELECT
        r.*,
        CONDITIONAL_CHANGE_EVENT(r.STATUS) OVER (
            PARTITION BY r.SUBSCRIPTION_ID ORDER BY r.VALID_FROM
        )                                                           AS RUN_NO
    FROM status_raw r
),

status_clean AS (
    SELECT
        SUBSCRIPTION_ID,
        STATUS,
        MIN(VALID_FROM)                                             AS VALID_FROM,
        MAX(VALID_TO)                                               AS VALID_TO,
        COUNT(*)                                                    AS RAW_ROWS
    FROM status_runs
    GROUP BY SUBSCRIPTION_ID, STATUS, RUN_NO
),

/* ---------- 2. pattern match the journeys ---------- */
journeys_all AS (
    SELECT *
    FROM status_clean
    MATCH_RECOGNIZE (
        PARTITION BY SUBSCRIPTION_ID
        ORDER BY VALID_FROM
        MEASURES
            MATCH_NUMBER()                                          AS MATCH_NO,
            FIRST(A.VALID_FROM)                                     AS JOURNEY_START,
            LAST(VALID_FROM)                                        AS JOURNEY_END,
            FIRST(S.VALID_FROM)                                     AS FIRST_SUSPEND_AT,
            LAST(C.VALID_FROM)                                      AS CHURN_AT,
            LAST(R.VALID_FROM)                                      AS REACTIVATED_AT,
            COUNT(S.*)                                              AS SUSPEND_STEPS,
            COUNT(C.*)                                              AS CHURN_STEPS,
            COUNT(R.*)                                              AS REACTIVATIONS,
            COUNT(*)                                                AS TOTAL_STEPS,
            SUM(RAW_ROWS)                                           AS RAW_HISTORY_ROWS,
            CLASSIFIER()                                            AS LAST_STEP
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN ( A S* C R | A S+ C | A S+ R | A C )
        DEFINE
            A AS STATUS = 'ACTIVE',
            S AS STATUS = 'SUSPENDED',
            C AS STATUS = 'CHURNED',
            R AS STATUS = 'ACTIVE'
    )
),

journeys AS (
    SELECT
        DATE_TRUNC('month', j.JOURNEY_END)::DATE                    AS JOURNEY_MONTH,
        j.SUBSCRIPTION_ID,
        j.MATCH_NO,
        j.JOURNEY_START,
        j.JOURNEY_END,
        j.FIRST_SUSPEND_AT,
        j.CHURN_AT,
        j.REACTIVATED_AT,
        j.SUSPEND_STEPS,
        j.CHURN_STEPS,
        j.REACTIVATIONS,
        j.TOTAL_STEPS,
        j.RAW_HISTORY_ROWS,
        j.LAST_STEP,
        CASE
            WHEN j.CHURN_STEPS > 0 AND j.REACTIVATIONS > 0 THEN 'WIN_BACK_AFTER_CHURN'
            WHEN j.SUSPEND_STEPS > 0 AND j.CHURN_STEPS > 0  THEN 'LOST_AFTER_SUSPEND'
            WHEN j.SUSPEND_STEPS > 0 AND j.REACTIVATIONS > 0 THEN 'RECOVERED_FROM_SUSPEND'
            ELSE 'DIRECT_CHURN'
        END                                                         AS JOURNEY_TYPE,
        IFF(j.CHURN_STEPS > 0 AND j.REACTIVATIONS = 0, TRUE, FALSE) AS ENDED_CHURNED,
        DATEDIFF('day', j.JOURNEY_START, j.JOURNEY_END)             AS JOURNEY_DAYS,
        DATEDIFF('day', j.FIRST_SUSPEND_AT, COALESCE(j.CHURN_AT, j.REACTIVATED_AT)) AS DAYS_IN_SUSPENSION,
        DATEDIFF('day', j.CHURN_AT, j.REACTIVATED_AT)               AS DAYS_CHURNED_BEFORE_WINBACK,
        j.JOURNEY_END::DATE                                         AS JOURNEY_END_DATE,
        DATE_TRUNC('month', j.JOURNEY_END)::DATE                    AS RUN_MONTH,
        LAST_DAY(j.JOURNEY_END)::DATE                               AS MONTH_END
    FROM journeys_all j
    CROSS JOIN params p
    WHERE j.JOURNEY_END >= p.WINDOW_START
      AND j.JOURNEY_END <  p.WINDOW_END
),

/* ---------- 3. subscriber, account and customer ---------- */
subscriber AS (
    SELECT
        s.SUBSCRIPTION_ID,
        s.ACCOUNT_ID,
        s.PLAN_ID,
        s.MSISDN,
        s.HOME_CELL_ID,
        s.DEVICE_ID,
        s.PARTNER_BRAND_ID,
        s.MONTHLY_CHARGES_PHP                                       AS MONTHLY_CHARGES,
        s.TOTAL_CHARGES_PHP                                         AS TOTAL_CHARGES,
        s.TENURE_MONTHS,
        s.INTERNET_SERVICE,
        s.HAS_PHONE_SERVICE,
        s.STREAMING_TV,
        s.STREAMING_MOVIES,
        s.IS_DELETED,
        a.CUSTOMER_ID,
        a.PAYMENT_METHOD,
        a.IS_PAPERLESS_BILLING                                      AS PAPERLESS_BILLING,
        a.BILLING_CYCLE_DAY,
        c.GENDER,
        c.IS_SENIOR,
        c.HAS_PARTNER,
        c.HAS_DEPENDENTS,
        c.PREFERRED_LANGUAGE,
        c.KYC_DOC_TYPE,
        c.KYC_LIVENESS_SCORE,
        c.KYC_HAS_ADDRESS_PROOF                                     AS KYC_ADDRESS_PROOF,
        c.DATE_OF_BIRTH,
        COALESCE(c.IS_TEST_CUSTOMER, FALSE)                         AS IS_TEST_CUSTOMER
    FROM {{ ref('slv_subscription') }} s
    LEFT JOIN {{ ref('slv_account') }} a
           ON a.ACCOUNT_ID = s.ACCOUNT_ID
    LEFT JOIN {{ ref('slv_customer') }} c
           ON c.CUSTOMER_ID = a.CUSTOMER_ID
    WHERE s.SUBSCRIPTION_ID IN (SELECT SUBSCRIPTION_ID FROM journeys)
),

plan_at_end AS (
    SELECT
        j.SUBSCRIPTION_ID,
        j.JOURNEY_START,
        pc.PLAN_NAME,
        pc.CONTRACT_TYPE                                            AS PLAN_CONTRACT_TYPE,
        pc.BASE_PRICE_PHP                                           AS BASE_PRICE,
        pc.SPEED_MBPS                                               AS PLAN_SPEED_MBPS,
        pc.DATA_CAP_GB                                              AS PLAN_DATA_CAP_GB,
        pc.IS_PROMO                                                 AS PLAN_IS_PROMO
    FROM journeys j
    JOIN subscriber s
      ON s.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID
    JOIN {{ ref('slv_plan') }} pc
      ON pc.PLAN_ID = s.PLAN_ID
     AND j.JOURNEY_END_DATE BETWEEN pc.EFFECTIVE_FROM AND COALESCE(pc.EFFECTIVE_TO, '9999-12-31'::DATE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY j.SUBSCRIPTION_ID, j.JOURNEY_START ORDER BY pc.EFFECTIVE_FROM DESC) = 1
),

contract_at_start AS (
    SELECT
        j.SUBSCRIPTION_ID,
        j.JOURNEY_START,
        ct.CONTRACT_ID,
        ct.CONTRACT_TYPE,
        ct.IS_AUTO_RENEW                                            AS AUTO_RENEW,
        ct.END_DATE                                                 AS CONTRACT_END_DATE,
        ct.TERMINATION_REASON,
        DATEDIFF('day', j.JOURNEY_END_DATE, ct.END_DATE)            AS CONTRACT_DAYS_LEFT
    FROM journeys j
    JOIN {{ ref('slv_contract') }} ct
      ON ct.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID
     AND ct.START_DATE <= j.JOURNEY_START::DATE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY j.SUBSCRIPTION_ID, j.JOURNEY_START ORDER BY ct.START_DATE DESC) = 1
),

home_geo AS (
    SELECT
        s.SUBSCRIPTION_ID,
        cs.SITE_NAME                                                AS HOME_SITE_NAME,
        cs.RADIO_TYPE                                               AS HOME_RADIO_TYPE,
        g.REGION_ID                                                 AS HOME_REGION_ID,
        COALESCE(g.REGION_NAME, 'UNKNOWN')                          AS HOME_REGION_NAME,
        g.REGION_CITY                                               AS HOME_CITY,
        g.ISLAND_GROUP                                              AS HOME_ISLAND_GROUP
    FROM subscriber s
    LEFT JOIN {{ ref('slv_cell_site') }} cs
           ON cs.CELL_ID = s.HOME_CELL_ID
    LEFT JOIN {{ ref('slv_region') }} g
           ON g.REGION_ID = cs.REGION_ID
),

partner AS (
    SELECT
        m.SUBSCRIPTION_ID,
        b.BRAND_NAME,
        b.OPERATOR_TYPE,
        b.REVENUE_SHARE_PCT,
        m.WHOLESALE_PLAN
    FROM {{ ref('slv_partner_subscriber_map') }} m
    JOIN {{ ref('slv_partner_brand') }} b
      ON b.PARTNER_BRAND_ID = m.PARTNER_BRAND_ID
    WHERE m.SUBSCRIPTION_ID IN (SELECT SUBSCRIPTION_ID FROM journeys)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY m.SUBSCRIPTION_ID ORDER BY m.ONBOARDED_AT_UTC DESC) = 1
),

/* ---------- 4. activity around the journey ---------- */
revenue_window AS (
    SELECT
        j.SUBSCRIPTION_ID,
        j.JOURNEY_START,
        SUM(IFF(r.BUSINESS_DATE <  j.JOURNEY_START::DATE
              AND r.BUSINESS_DATE >= DATEADD('day', -90, j.JOURNEY_START::DATE), r.REVENUE_PHP, 0)) AS REVENUE_90D_BEFORE,
        SUM(IFF(r.BUSINESS_DATE BETWEEN j.JOURNEY_START::DATE AND j.JOURNEY_END_DATE, r.REVENUE_PHP, 0)) AS REVENUE_DURING,
        SUM(IFF(r.BUSINESS_DATE >  j.JOURNEY_END_DATE, r.REVENUE_PHP, 0))                     AS REVENUE_AFTER_END,
        SUM(IFF(r.REVENUE_SOURCE = 'PLAN' AND r.BUSINESS_DATE < j.JOURNEY_START::DATE, r.REVENUE_PHP, 0)) AS PLAN_REVENUE_BEFORE,
        SUM(IFF(r.REVENUE_SOURCE = 'VAS' AND r.BUSINESS_DATE < j.JOURNEY_START::DATE, r.REVENUE_PHP, 0))  AS VAS_REVENUE_BEFORE,
        COUNT(DISTINCT IFF(r.BUSINESS_DATE < j.JOURNEY_START::DATE, r.BUSINESS_DATE, NULL))   AS REVENUE_DAYS_BEFORE
    FROM journeys j
    JOIN {{ ref('slv_daily_revenue') }} r
      ON r.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID
     AND r.BUSINESS_DATE >= DATEADD('day', -90, j.JOURNEY_START::DATE)
     AND r.BUSINESS_DATE <  DATEADD('month', 1, j.RUN_MONTH)
    GROUP BY j.SUBSCRIPTION_ID, j.JOURNEY_START
),

usage_trend AS (
    SELECT
        j.SUBSCRIPTION_ID,
        j.JOURNEY_START,
        SUM(u.DATA_MB)                                              AS DATA_MB_60D,
        SUM(u.VOICE_MIN)                                            AS VOICE_MIN_60D,
        SUM(u.SMS_COUNT)                                            AS SMS_60D,
        COUNT(*)                                                    AS ACTIVE_DAYS_60D,
        REGR_SLOPE(u.DATA_MB, DATEDIFF('day', DATEADD('day', -60, j.JOURNEY_END_DATE), u.EVENT_DATE))   AS DATA_MB_SLOPE,
        REGR_SLOPE(u.VOICE_MIN, DATEDIFF('day', DATEADD('day', -60, j.JOURNEY_END_DATE), u.EVENT_DATE)) AS VOICE_MIN_SLOPE,
        MEDIAN(u.DATA_MB)                                           AS DATA_MB_MEDIAN_DAY,
        MAX(u.EVENT_DATE)                                           AS LAST_USAGE_DATE
    FROM journeys j
    JOIN {{ ref('fct_subscriber_usage_daily') }} u
      ON u.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID
     AND u.EVENT_DATE >  DATEADD('day', -60, j.JOURNEY_END_DATE)
     AND u.EVENT_DATE <= j.JOURNEY_END_DATE
    GROUP BY j.SUBSCRIPTION_ID, j.JOURNEY_START
),

tickets AS (
    SELECT
        j.SUBSCRIPTION_ID,
        j.JOURNEY_START,
        COUNT(*)                                                    AS TICKETS_90D,
        COUNT_IF(t.PRIORITY = 'P1')                                 AS P1_TICKETS_90D,
        COUNT_IF(t.STATUS = 'OPEN')                                 AS OPEN_TICKETS,
        COUNT_IF(t.CATEGORY IN ('NO_INTERNET', 'SLOW_SPEED'))       AS NETWORK_TICKETS_90D,
        COUNT_IF(t.CATEGORY = 'BILLING')                            AS BILLING_TICKETS_90D,
        ARRAY_AGG(DISTINCT t.CATEGORY) WITHIN GROUP (ORDER BY t.CATEGORY) AS TICKET_CATEGORIES,
        MAX_BY(t.CATEGORY, t.OPENED_AT_UTC)                         AS LAST_TICKET_CATEGORY,
        AVG(t.RESOLUTION_HOURS)                                     AS AVG_HOURS_TO_RESOLVE
    FROM journeys j
    JOIN {{ ref('slv_ticket') }} t
      ON t.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID
     AND t.OPENED_AT_LOCAL >= DATEADD('day', -90, j.JOURNEY_END)
     AND t.OPENED_AT_LOCAL <  DATEADD('day', 1, j.JOURNEY_END)
    GROUP BY j.SUBSCRIPTION_ID, j.JOURNEY_START
),

nps AS (
    SELECT
        j.SUBSCRIPTION_ID,
        j.JOURNEY_START,
        MAX_BY(n.NPS_SCORE, n.RESPONDED_AT_UTC)                     AS LAST_NPS_SCORE,
        MAX_BY(n.WOULD_RECOMMEND, n.RESPONDED_AT_UTC)               AS LAST_WOULD_RECOMMEND,
        AVG(n.Q_SPEED)                                              AS AVG_SPEED_RATING,
        COUNT(*)                                                    AS SURVEYS
    FROM journeys j
    JOIN {{ ref('slv_nps_survey') }} n
      ON n.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID
     AND n.RESPONSE_DATE <= j.JOURNEY_END_DATE
    GROUP BY j.SUBSCRIPTION_ID, j.JOURNEY_START
),

churn_score AS (
    SELECT
        cs.SUBSCRIPTION_ID,
        cs.CHURN_PROBABILITY,
        cs.MODEL_VERSION,
        cs.SCORE_DATE,
        cs.CHURN_BAND
    FROM {{ ref('slv_churn_score') }} cs
    WHERE cs.SUBSCRIPTION_ID IN (SELECT SUBSCRIPTION_ID FROM journeys)
),

segment AS (
    SELECT
        j.SUBSCRIPTION_ID,
        j.JOURNEY_START,
        LISTAGG(DISTINCT sm.SEGMENT_CODE, '|') WITHIN GROUP (ORDER BY sm.SEGMENT_CODE) AS SEGMENTS_AT_END,
        MAX(IFF(sm.SEGMENT_CODE = 'AT_RISK', 1, 0))                 AS WAS_AT_RISK,
        MAX(IFF(sm.SEGMENT_CODE = 'VALUE_HIGH', 1, 0))              AS WAS_HIGH_VALUE
    FROM journeys j
    JOIN {{ ref('slv_segment_membership') }} sm
      ON sm.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID
     AND j.JOURNEY_END_DATE BETWEEN sm.VALID_FROM AND sm.VALID_TO
    GROUP BY j.SUBSCRIPTION_ID, j.JOURNEY_START
),

recharges AS (
    SELECT
        j.SUBSCRIPTION_ID,
        j.JOURNEY_START,
        COUNT(*)                                                    AS RECHARGES_30D,
        SUM(rc.RECHARGE_AMOUNT_PHP)                                 AS RECHARGE_PHP_30D,
        MAX_BY(rc.RECHARGE_CHANNEL, rc.RECHARGED_AT_LOCAL)          AS LAST_RECHARGE_CHANNEL,
        MAX(rc.RECHARGED_AT_LOCAL)                                  AS LAST_RECHARGE_AT,
        MODE(rc.PACK_CODE)                                          AS TOP_PACK
    FROM journeys j
    JOIN {{ ref('slv_recharge') }} rc
      ON rc.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID
     AND rc.RECHARGED_AT_LOCAL >= DATEADD('day', -30, j.JOURNEY_END)
     AND rc.RECHARGED_AT_LOCAL <  DATEADD('day', 1, j.JOURNEY_END)
    GROUP BY j.SUBSCRIPTION_ID, j.JOURNEY_START
),

dunning AS (
    SELECT
        s.SUBSCRIPTION_ID,
        j.JOURNEY_START,
        MAX(d.DUNNING_STAGE)                                        AS MAX_DUNNING_STAGE,
        COUNT_IF(d.DUNNING_ACTION = 'SERVICE_BARRED')               AS BARRING_EVENTS,
        SUM(d.AMOUNT_DUE_PHP)                                       AS AMOUNT_DUE_IN_DUNNING
    FROM journeys j
    JOIN subscriber s
      ON s.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID
    JOIN {{ ref('slv_dunning_event') }} d
      ON d.ACCOUNT_ID = s.ACCOUNT_ID
     AND {{ utc_to_local('d.ACTION_AT_UTC') }} >= DATEADD('day', -60, j.JOURNEY_END)
     AND {{ utc_to_local('d.ACTION_AT_UTC') }} <  DATEADD('day', 1, j.JOURNEY_END)
    GROUP BY s.SUBSCRIPTION_ID, j.JOURNEY_START
),

outages AS (
    SELECT
        j.SUBSCRIPTION_ID,
        j.JOURNEY_START,
        COUNT(DISTINCT oi.INCIDENT_ID)                              AS OUTAGES_60D,
        SUM(oim.IMPACT_MINUTES)                                     AS OUTAGE_MINUTES_60D,
        BOOLOR_AGG(oim.IS_CREDIT_ELIGIBLE)                          AS ANY_CREDIT_ELIGIBLE,
        MAX_BY(oi.ROOT_CAUSE, oim.IMPACT_MINUTES)                   AS MAIN_OUTAGE_CAUSE
    FROM journeys j
    JOIN {{ ref('slv_outage_impact') }} oim
      ON oim.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID
    JOIN {{ ref('slv_outage_incident') }} oi
      ON oi.INCIDENT_ID = oim.INCIDENT_ID
     AND oi.EVENT_DATE >  DATEADD('day', -60, j.JOURNEY_END_DATE)
     AND oi.EVENT_DATE <= j.JOURNEY_END_DATE
    GROUP BY j.SUBSCRIPTION_ID, j.JOURNEY_START
),

/* ---------- 5. assemble ---------- */
assembled AS (
    SELECT
        j.JOURNEY_MONTH,
        j.SUBSCRIPTION_ID,
        j.MATCH_NO,
        j.JOURNEY_TYPE,
        j.ENDED_CHURNED,
        j.JOURNEY_START,
        j.JOURNEY_END,
        j.JOURNEY_DAYS,
        j.FIRST_SUSPEND_AT,
        j.CHURN_AT,
        j.REACTIVATED_AT,
        j.DAYS_IN_SUSPENSION,
        j.DAYS_CHURNED_BEFORE_WINBACK,
        j.SUSPEND_STEPS,
        j.CHURN_STEPS,
        j.REACTIVATIONS,
        j.TOTAL_STEPS,
        j.RAW_HISTORY_ROWS,
        j.LAST_STEP,
        s.ACCOUNT_ID,
        s.CUSTOMER_ID,
        s.PLAN_ID,
        pl.PLAN_NAME,
        pl.PLAN_CONTRACT_TYPE,
        pl.BASE_PRICE,
        pl.PLAN_SPEED_MBPS,
        pl.PLAN_DATA_CAP_GB,
        COALESCE(pl.PLAN_IS_PROMO, FALSE)                           AS PLAN_IS_PROMO,
        ct.CONTRACT_ID,
        ct.CONTRACT_TYPE,
        ct.AUTO_RENEW,
        ct.CONTRACT_END_DATE,
        ct.CONTRACT_DAYS_LEFT,
        ct.TERMINATION_REASON,
        s.TENURE_MONTHS,
        CASE
            WHEN s.TENURE_MONTHS < 6  THEN '0-5M'
            WHEN s.TENURE_MONTHS < 12 THEN '6-11M'
            WHEN s.TENURE_MONTHS < 24 THEN '12-23M'
            WHEN s.TENURE_MONTHS < 48 THEN '24-47M'
            ELSE '48M+'
        END                                                         AS TENURE_BAND,
        s.MONTHLY_CHARGES,
        s.TOTAL_CHARGES,
        s.INTERNET_SERVICE,
        s.PAYMENT_METHOD,
        s.PAPERLESS_BILLING,
        s.GENDER,
        s.IS_SENIOR,
        s.PREFERRED_LANGUAGE,
        DATEDIFF('year', s.DATE_OF_BIRTH, j.JOURNEY_END_DATE)       AS AGE_AT_END,
        s.KYC_DOC_TYPE,
        s.KYC_LIVENESS_SCORE,
        g.HOME_REGION_ID,
        COALESCE(g.HOME_REGION_NAME, 'UNKNOWN')                     AS HOME_REGION_NAME,
        g.HOME_CITY,
        g.HOME_ISLAND_GROUP,
        g.HOME_RADIO_TYPE,
        pa.BRAND_NAME                                               AS PARTNER_BRAND,
        pa.WHOLESALE_PLAN,
        ZEROIFNULL(rv.REVENUE_90D_BEFORE)                           AS REVENUE_90D_BEFORE,
        ZEROIFNULL(rv.REVENUE_DURING)                               AS REVENUE_DURING,
        ZEROIFNULL(rv.REVENUE_AFTER_END)                            AS REVENUE_AFTER_END,
        ZEROIFNULL(rv.PLAN_REVENUE_BEFORE)                          AS PLAN_REVENUE_BEFORE,
        ZEROIFNULL(rv.VAS_REVENUE_BEFORE)                           AS VAS_REVENUE_BEFORE,
        ROUND(DIV0(rv.REVENUE_90D_BEFORE, rv.REVENUE_DAYS_BEFORE) * 30, 2) AS ARPU_30D_BEFORE,
        ZEROIFNULL(ut.DATA_MB_60D)                                  AS DATA_MB_60D,
        ZEROIFNULL(ut.VOICE_MIN_60D)                                AS VOICE_MIN_60D,
        ZEROIFNULL(ut.SMS_60D)                                      AS SMS_60D,
        ZEROIFNULL(ut.ACTIVE_DAYS_60D)                              AS ACTIVE_DAYS_60D,
        ROUND(ut.DATA_MB_SLOPE, 4)                                  AS DATA_MB_SLOPE,
        ROUND(ut.VOICE_MIN_SLOPE, 4)                                AS VOICE_MIN_SLOPE,
        ut.DATA_MB_MEDIAN_DAY,
        ut.LAST_USAGE_DATE,
        DATEDIFF('day', ut.LAST_USAGE_DATE, j.JOURNEY_END_DATE)     AS DAYS_SINCE_LAST_USAGE,
        ZEROIFNULL(tk.TICKETS_90D)                                  AS TICKETS_90D,
        ZEROIFNULL(tk.P1_TICKETS_90D)                               AS P1_TICKETS_90D,
        ZEROIFNULL(tk.OPEN_TICKETS)                                 AS OPEN_TICKETS,
        ZEROIFNULL(tk.NETWORK_TICKETS_90D)                          AS NETWORK_TICKETS_90D,
        ZEROIFNULL(tk.BILLING_TICKETS_90D)                          AS BILLING_TICKETS_90D,
        tk.TICKET_CATEGORIES,
        tk.LAST_TICKET_CATEGORY,
        ROUND(tk.AVG_HOURS_TO_RESOLVE, 1)                           AS AVG_HOURS_TO_RESOLVE,
        n.LAST_NPS_SCORE,
        n.LAST_WOULD_RECOMMEND,
        ROUND(n.AVG_SPEED_RATING, 2)                                AS AVG_SPEED_RATING,
        CASE
            WHEN n.LAST_NPS_SCORE >= 9 THEN 'PROMOTER'
            WHEN n.LAST_NPS_SCORE >= 7 THEN 'PASSIVE'
            WHEN n.LAST_NPS_SCORE IS NOT NULL THEN 'DETRACTOR'
            ELSE 'NO_RESPONSE'
        END                                                         AS NPS_GROUP,
        cs.CHURN_PROBABILITY,
        cs.CHURN_BAND,
        cs.MODEL_VERSION                                            AS CHURN_MODEL_VERSION,
        sg.SEGMENTS_AT_END,
        ZEROIFNULL(sg.WAS_AT_RISK)                                  AS WAS_AT_RISK,
        ZEROIFNULL(sg.WAS_HIGH_VALUE)                               AS WAS_HIGH_VALUE,
        ZEROIFNULL(rc.RECHARGES_30D)                                AS RECHARGES_30D,
        ZEROIFNULL(rc.RECHARGE_PHP_30D)                             AS RECHARGE_PHP_30D,
        rc.LAST_RECHARGE_CHANNEL,
        rc.TOP_PACK,
        ZEROIFNULL(dn.MAX_DUNNING_STAGE)                            AS MAX_DUNNING_STAGE,
        ZEROIFNULL(dn.BARRING_EVENTS)                               AS BARRING_EVENTS,
        ZEROIFNULL(dn.AMOUNT_DUE_IN_DUNNING)                        AS AMOUNT_DUE_IN_DUNNING,
        ZEROIFNULL(ou.OUTAGES_60D)                                  AS OUTAGES_60D,
        ZEROIFNULL(ou.OUTAGE_MINUTES_60D)                           AS OUTAGE_MINUTES_60D,
        COALESCE(ou.ANY_CREDIT_ELIGIBLE, FALSE)                     AS ANY_CREDIT_ELIGIBLE,
        ou.MAIN_OUTAGE_CAUSE,
        s.IS_DELETED,
        s.IS_TEST_CUSTOMER
    FROM journeys j
    JOIN subscriber s              ON s.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID
    LEFT JOIN plan_at_end pl       ON pl.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID AND pl.JOURNEY_START = j.JOURNEY_START
    LEFT JOIN contract_at_start ct ON ct.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID AND ct.JOURNEY_START = j.JOURNEY_START
    LEFT JOIN home_geo g           ON g.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID
    LEFT JOIN partner pa           ON pa.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID
    LEFT JOIN revenue_window rv    ON rv.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID AND rv.JOURNEY_START = j.JOURNEY_START
    LEFT JOIN usage_trend ut       ON ut.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID AND ut.JOURNEY_START = j.JOURNEY_START
    LEFT JOIN tickets tk           ON tk.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID AND tk.JOURNEY_START = j.JOURNEY_START
    LEFT JOIN nps n                ON n.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID AND n.JOURNEY_START = j.JOURNEY_START
    LEFT JOIN churn_score cs       ON cs.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID
    LEFT JOIN segment sg           ON sg.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID AND sg.JOURNEY_START = j.JOURNEY_START
    LEFT JOIN recharges rc         ON rc.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID AND rc.JOURNEY_START = j.JOURNEY_START
    LEFT JOIN dunning dn           ON dn.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID AND dn.JOURNEY_START = j.JOURNEY_START
    LEFT JOIN outages ou           ON ou.SUBSCRIPTION_ID = j.SUBSCRIPTION_ID AND ou.JOURNEY_START = j.JOURNEY_START
)

SELECT
    HASH(a.SUBSCRIPTION_ID, a.JOURNEY_START)                        AS JOURNEY_KEY,
    a.*,
    /* likely driver of the journey, first rule that matches wins */
    CASE
        WHEN a.JOURNEY_TYPE IN ('LOST_AFTER_SUSPEND', 'RECOVERED_FROM_SUSPEND') AND a.MAX_DUNNING_STAGE >= 2 THEN 'NON_PAYMENT'
        WHEN a.OUTAGE_MINUTES_60D >= 240 OR a.NETWORK_TICKETS_90D >= 2                                    THEN 'NETWORK_EXPERIENCE'
        WHEN a.BILLING_TICKETS_90D >= 1                                                                   THEN 'BILLING_DISPUTE'
        WHEN a.CONTRACT_DAYS_LEFT BETWEEN -30 AND 30 AND NOT COALESCE(a.AUTO_RENEW, FALSE)               THEN 'CONTRACT_END'
        WHEN a.DATA_MB_SLOPE < 0 AND a.ACTIVE_DAYS_60D < 20                                               THEN 'DISENGAGED'
        WHEN EQUAL_NULL(a.NPS_GROUP, 'DETRACTOR')                                                         THEN 'UNHAPPY'
        ELSE 'UNKNOWN'
    END                                                             AS LIKELY_DRIVER,
    ROUND(
          0.30 * LEAST(a.MAX_DUNNING_STAGE / 3, 1)
        + 0.20 * LEAST(a.OUTAGE_MINUTES_60D / 600, 1)
        + 0.20 * LEAST(a.TICKETS_90D / 5, 1)
        + 0.15 * IFF(a.DATA_MB_SLOPE < 0, 1, 0)
        + 0.15 * COALESCE(a.CHURN_PROBABILITY, 0)
    , 4)                                                            AS RISK_SIGNAL_SCORE,
    RANK() OVER (PARTITION BY a.JOURNEY_MONTH, a.JOURNEY_TYPE ORDER BY a.REVENUE_90D_BEFORE DESC NULLS LAST) AS VALUE_RANK_IN_TYPE,
    ROUND(RATIO_TO_REPORT(a.REVENUE_90D_BEFORE) OVER (PARTITION BY a.JOURNEY_MONTH, a.JOURNEY_TYPE), 6)      AS REVENUE_SHARE_IN_TYPE,
    OBJECT_CONSTRUCT_KEEP_NULL(
        'dunning_stage', a.MAX_DUNNING_STAGE,
        'outage_minutes', a.OUTAGE_MINUTES_60D,
        'tickets_90d', a.TICKETS_90D,
        'data_slope', a.DATA_MB_SLOPE,
        'nps', a.LAST_NPS_SCORE,
        'churn_probability', a.CHURN_PROBABILITY,
        'segments', a.SEGMENTS_AT_END
    )                                                               AS SIGNALS,
    CURRENT_TIMESTAMP()                                             AS LOADED_AT
FROM assembled a
WHERE NOT a.IS_TEST_CUSTOMER
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.SUBSCRIPTION_ID, a.JOURNEY_START ORDER BY a.JOURNEY_END DESC) = 1
