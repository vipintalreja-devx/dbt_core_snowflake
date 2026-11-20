{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['outage_month'],
    on_schema_change = 'sync_all_columns',
    transient = false,
    copy_grants = true,
    cluster_by = ['outage_month', 'region_id'],
    tags = ['gold', 'advanced', 'h11', 'outage', 'geo'],
    query_tag = 'dbt_telco_adv_h11'
  )
}}
{#-
  adv_outage_geo_impact  -  Outage geo impact check
  ------------------------------------------------------------------------------------------
  Ported from x_h11_outage_geo_impact_check (PIPE_X.SP_X_H11_OUTAGE_GEO_IMPACT_CHECK).

  For every outage incident detected in each month of the window, finds the subscribers
  whose service address lies inside the affected area (ST_INTERSECTS) and compares them with
  the subscribers recorded as impacted. Each incident also gets: cells and network elements
  inside the area, alarms raised in the area during the incident, tickets opened by
  affected subscribers while it lasted, outage credits issued afterwards, revenue at risk,
  the geometry of the area (size, centroid, population inside), 12-month recurrence and
  concurrent incidents in the region.

  Grain     : incident_id (outage_month = Manila month the incident was detected).
  Window    : months from var('history_start_date'); incremental restates the last loaded
              month and later. 12-month recurrence reads full silver incident history.
  Changed vs original: one RUN_MONTH -> all months (region-month stats and ranks partition by
              month); incident times are UTC in silver and compared on UTC; credits use the
              positive silver credit_amount_php (original summed the negative AMOUNT);
              revenue baseline = 30 days before each incident's month.
  Snowflake : ST_INTERSECTS, ST_AREA, ST_CENTROID, ST_X / ST_Y, CONVERT_TIMEZONE, ARRAY_AGG
              DISTINCT, ARRAY_INTERSECTION, ARRAY_SIZE, MODE, MEDIAN, MAX_BY, LISTAGG DISTINCT,
              DIV0, IFF, ZEROIFNULL, QUALIFY, copy_grants.
  Sources (15): slv_outage_incident (2x), slv_outage_impact, slv_service_instance,
              ref.geo_zip_population (bronze, 2x), slv_region, slv_cell_site,
              slv_network_element, slv_alarm_event, slv_ticket, slv_adjustment,
              slv_subscription, slv_plan, slv_daily_revenue.
-#}

WITH params AS (
    SELECT {{ f3_month_lower_bound('outage_month') }}               AS WINDOW_START
),

/* ---------- 1. incidents of the window and their area ---------- */
incidents AS (
    SELECT
        DATE_TRUNC('month', i.EVENT_DATE)::DATE                     AS OUTAGE_MONTH,
        i.INCIDENT_ID,
        i.REGION_ID,
        i.SEVERITY,
        i.ROOT_CAUSE,
        i.DETECTED_AT_UTC                                           AS DETECTED_AT,
        i.RESTORED_AT_UTC                                           AS RESTORED_AT,
        COALESCE(i.RESTORED_AT_UTC, DATEADD('hour', 24, i.DETECTED_AT_UTC)) AS EFFECTIVE_END_AT,
        DATEDIFF('minute', i.DETECTED_AT_UTC, COALESCE(i.RESTORED_AT_UTC, DATEADD('hour', 24, i.DETECTED_AT_UTC))) AS DURATION_MIN,
        i.IS_ONGOING                                                AS STILL_OPEN,
        i.AFFECTED_AREA,
        ROUND(ST_AREA(i.AFFECTED_AREA) / 1e6, 4)                    AS AREA_KM2,
        ST_CENTROID(i.AFFECTED_AREA)                                AS AREA_CENTROID,
        i.EVENT_DATE                                                AS DETECTED_DATE
    FROM {{ ref('slv_outage_incident') }} i
    CROSS JOIN params p
    WHERE i.EVENT_DATE >= p.WINDOW_START
      AND i.AFFECTED_AREA IS NOT NULL
),

/* ---------- 2. service addresses (one per subscriber) ---------- */
addresses AS (
    SELECT
        si.SUBSCRIPTION_ID,
        si.SERVICE_TYPE,
        si.SERVICE_LOCATION                                         AS LOCATION,
        si.CITY,
        si.ZIP_CODE,
        z.REGION_ID
    FROM {{ ref('slv_service_instance') }} si
    JOIN {{ source('ref', 'geo_zip_population') }} z
      ON z.ZIP_CODE = si.ZIP_CODE
    WHERE si.SERVICE_LOCATION IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY si.SUBSCRIPTION_ID ORDER BY si.IS_ACTIVE DESC, si.VALIDATED_ON DESC NULLS LAST) = 1
),

geo_hits AS (
    SELECT
        i.INCIDENT_ID,
        a.SUBSCRIPTION_ID,
        a.SERVICE_TYPE,
        a.ZIP_CODE
    FROM incidents i
    JOIN addresses a
      ON a.REGION_ID = i.REGION_ID
    WHERE ST_INTERSECTS(i.AFFECTED_AREA, a.LOCATION)
),

recorded AS (
    SELECT
        im.INCIDENT_ID,
        im.SUBSCRIPTION_ID,
        im.IMPACT_MINUTES,
        im.IS_CREDIT_ELIGIBLE                                       AS CREDIT_ELIGIBLE
    FROM {{ ref('slv_outage_impact') }} im
    JOIN incidents i
      ON i.INCIDENT_ID = im.INCIDENT_ID
),

matched AS (
    SELECT
        COALESCE(g.INCIDENT_ID, r.INCIDENT_ID)                      AS INCIDENT_ID,
        COALESCE(g.SUBSCRIPTION_ID, r.SUBSCRIPTION_ID)              AS SUBSCRIPTION_ID,
        IFF(g.SUBSCRIPTION_ID IS NOT NULL, 1, 0)                    AS IN_GEO,
        IFF(r.SUBSCRIPTION_ID IS NOT NULL, 1, 0)                    AS IN_RECORD,
        g.SERVICE_TYPE,
        r.IMPACT_MINUTES,
        r.CREDIT_ELIGIBLE
    FROM geo_hits g
    FULL OUTER JOIN recorded r
      ON r.INCIDENT_ID = g.INCIDENT_ID
     AND r.SUBSCRIPTION_ID = g.SUBSCRIPTION_ID
),

/* ---------- 3. what the affected subscribers are worth and how they reacted ---------- */
sub_value AS (
    SELECT
        m.INCIDENT_ID,
        m.SUBSCRIPTION_ID,
        s.ACCOUNT_ID,
        s.PLAN_ID,
        pc.CONTRACT_TYPE,
        AVG(dr.REVENUE_PHP)                                         AS AVG_DAILY_REVENUE_PHP
    FROM matched m
    JOIN incidents i
      ON i.INCIDENT_ID = m.INCIDENT_ID
    JOIN {{ ref('slv_subscription') }} s
      ON s.SUBSCRIPTION_ID = m.SUBSCRIPTION_ID
    LEFT JOIN {{ ref('slv_plan') }} pc
           ON pc.PLAN_ID = s.PLAN_ID
    LEFT JOIN {{ ref('slv_daily_revenue') }} dr
           ON dr.SUBSCRIPTION_ID = m.SUBSCRIPTION_ID
          AND dr.BUSINESS_DATE >= DATEADD('day', -30, i.OUTAGE_MONTH)
          AND dr.BUSINESS_DATE <  i.OUTAGE_MONTH
    GROUP BY m.INCIDENT_ID, m.SUBSCRIPTION_ID, s.ACCOUNT_ID, s.PLAN_ID, pc.CONTRACT_TYPE
),

tickets_during AS (
    SELECT
        i.INCIDENT_ID,
        COUNT(*)                                                    AS TICKETS_DURING,
        COUNT_IF(t.CATEGORY IN ('NO_INTERNET', 'SLOW_SPEED'))       AS NETWORK_TICKETS_DURING,
        COUNT(DISTINCT t.SUBSCRIPTION_ID)                           AS SUBS_WHO_CALLED,
        MODE(t.CHANNEL)                                             AS TOP_TICKET_CHANNEL
    FROM incidents i
    JOIN matched m
      ON m.INCIDENT_ID = i.INCIDENT_ID
    JOIN {{ ref('slv_ticket') }} t
      ON t.SUBSCRIPTION_ID = m.SUBSCRIPTION_ID
     AND t.OPENED_AT_UTC >= i.DETECTED_AT
     AND t.OPENED_AT_UTC <  DATEADD('hour', 6, i.EFFECTIVE_END_AT)
    GROUP BY i.INCIDENT_ID
),

credits AS (
    SELECT
        sv.INCIDENT_ID,
        COUNT(DISTINCT adj.ACCOUNT_ID)                              AS ACCOUNTS_CREDITED,
        SUM(adj.CREDIT_AMOUNT_PHP)                                  AS CREDIT_AMOUNT_PHP
    FROM sub_value sv
    JOIN incidents i
      ON i.INCIDENT_ID = sv.INCIDENT_ID
    JOIN {{ ref('slv_adjustment') }} adj
      ON adj.ACCOUNT_ID = sv.ACCOUNT_ID
     AND adj.REASON_CODE = 'OUTAGE_CREDIT'
     AND adj.ADJUSTMENT_DATE >= i.DETECTED_DATE
     AND adj.ADJUSTMENT_DATE <  DATEADD('day', 45, i.DETECTED_DATE)
    GROUP BY sv.INCIDENT_ID
),

/* ---------- 4. network view of the area ---------- */
cells_in_area AS (
    SELECT
        i.INCIDENT_ID,
        cs.CELL_ID,
        cs.RADIO_TYPE
    FROM incidents i
    JOIN {{ ref('slv_cell_site') }} cs
      ON cs.REGION_ID = i.REGION_ID
     AND cs.CELL_LOCATION IS NOT NULL
    WHERE ST_INTERSECTS(i.AFFECTED_AREA, cs.CELL_LOCATION)
),

cell_summary AS (
    SELECT
        c.INCIDENT_ID,
        COUNT(DISTINCT c.CELL_ID)                                   AS CELLS_IN_AREA,
        COUNT_IF(c.RADIO_TYPE = '5G')                               AS CELLS_5G_IN_AREA,
        ARRAY_AGG(DISTINCT c.CELL_ID)                               AS CELL_IDS,
        COUNT(DISTINCT ne.ELEMENT_ID)                               AS ELEMENTS_IN_AREA,
        MODE(ne.VENDOR)                                             AS MAIN_VENDOR
    FROM cells_in_area c
    LEFT JOIN {{ ref('slv_network_element') }} ne
           ON ne.CELL_ID = c.CELL_ID
    GROUP BY c.INCIDENT_ID
),

alarms AS (
    SELECT
        i.INCIDENT_ID,
        COUNT(*)                                                    AS ALARMS_DURING,
        COUNT_IF(ae.SEVERITY = 'CRITICAL')                          AS CRITICAL_ALARMS,
        COUNT_IF(ae.INCIDENT_ID = i.INCIDENT_ID)                    AS ALARMS_LINKED,
        MODE(ae.ALARM_CODE)                                         AS TOP_ALARM_CODE,
        MIN(ae.RAISED_AT_UTC)                                       AS FIRST_ALARM_AT,
        ARRAY_AGG(DISTINCT ae.CELL_ID)                              AS ALARM_CELL_IDS
    FROM incidents i
    JOIN cells_in_area c
      ON c.INCIDENT_ID = i.INCIDENT_ID
    JOIN {{ ref('slv_alarm_event') }} ae
      ON ae.CELL_ID = c.CELL_ID
     AND ae.RAISED_AT_UTC >= DATEADD('hour', -2, i.DETECTED_AT)
     AND ae.RAISED_AT_UTC <  i.EFFECTIVE_END_AT
    GROUP BY i.INCIDENT_ID
),

population AS (
    SELECT
        i.INCIDENT_ID,
        SUM(z.POPULATION)                                           AS POPULATION_IN_AREA,
        SUM(z.HOUSEHOLDS)                                           AS HOUSEHOLDS_IN_AREA,
        COUNT(*)                                                    AS ZIPS_IN_AREA
    FROM incidents i
    JOIN {{ source('ref', 'geo_zip_population') }} z
      ON z.REGION_ID = i.REGION_ID
     AND z.CENTROID IS NOT NULL
    WHERE ST_INTERSECTS(i.AFFECTED_AREA, z.CENTROID)
    GROUP BY i.INCIDENT_ID
),

/* ---------- 4b. history: earlier incidents in the same area (12 months) ---------- */
history AS (
    SELECT
        i.INCIDENT_ID,
        COUNT(h.INCIDENT_ID)                                        AS PRIOR_INCIDENTS_12M,
        COUNT_IF(h.ROOT_CAUSE = i.ROOT_CAUSE)                       AS PRIOR_SAME_CAUSE_12M,
        MAX(h.DETECTED_AT_UTC)                                      AS LAST_PRIOR_INCIDENT_AT,
        MAX_BY(h.ROOT_CAUSE, h.DETECTED_AT_UTC)                     AS LAST_PRIOR_ROOT_CAUSE,
        AVG(DATEDIFF('minute', h.DETECTED_AT_UTC, COALESCE(h.RESTORED_AT_UTC, h.DETECTED_AT_UTC))) AS PRIOR_AVG_DURATION_MIN,
        LISTAGG(DISTINCT h.SEVERITY, ',') WITHIN GROUP (ORDER BY h.SEVERITY) AS PRIOR_SEVERITIES
    FROM incidents i
    LEFT JOIN {{ ref('slv_outage_incident') }} h
           ON h.REGION_ID = i.REGION_ID
          AND h.INCIDENT_ID <> i.INCIDENT_ID
          AND h.DETECTED_AT_UTC >= DATEADD('month', -12, i.DETECTED_AT)
          AND h.DETECTED_AT_UTC <  i.DETECTED_AT
          AND h.AFFECTED_AREA IS NOT NULL
          AND ST_INTERSECTS(h.AFFECTED_AREA, i.AFFECTED_AREA)
    GROUP BY i.INCIDENT_ID
),

concurrent AS (
    SELECT
        a.INCIDENT_ID,
        COUNT(b.INCIDENT_ID)                                        AS CONCURRENT_INCIDENTS_IN_REGION,
        MIN(b.DETECTED_AT)                                          AS FIRST_CONCURRENT_DETECTED_AT
    FROM incidents a
    LEFT JOIN incidents b
           ON b.REGION_ID = a.REGION_ID
          AND b.INCIDENT_ID <> a.INCIDENT_ID
          AND b.DETECTED_AT < a.EFFECTIVE_END_AT
          AND b.EFFECTIVE_END_AT > a.DETECTED_AT
    GROUP BY a.INCIDENT_ID
),

region_month AS (
    SELECT
        i.OUTAGE_MONTH,
        i.REGION_ID,
        COUNT(*)                                                    AS REGION_INCIDENTS_IN_MONTH,
        SUM(i.DURATION_MIN)                                         AS REGION_OUTAGE_MIN_IN_MONTH,
        MEDIAN(i.DURATION_MIN)                                      AS REGION_MEDIAN_DURATION_MIN
    FROM incidents i
    GROUP BY i.OUTAGE_MONTH, i.REGION_ID
),

/* ---------- 5. per incident ---------- */
per_incident AS (
    SELECT
        m.INCIDENT_ID,
        SUM(m.IN_GEO)                                               AS GEO_SUBSCRIBERS,
        SUM(m.IN_RECORD)                                            AS RECORDED_SUBSCRIBERS,
        SUM(IFF(m.IN_GEO = 1 AND m.IN_RECORD = 1, 1, 0))            AS IN_BOTH,
        SUM(IFF(m.IN_GEO = 1 AND m.IN_RECORD = 0, 1, 0))            AS GEO_ONLY,
        SUM(IFF(m.IN_GEO = 0 AND m.IN_RECORD = 1, 1, 0))            AS RECORDED_ONLY,
        COUNT_IF(m.SERVICE_TYPE = 'FTTH')                           AS FTTH_SUBS_IN_AREA,
        SUM(ZEROIFNULL(m.IMPACT_MINUTES))                           AS RECORDED_IMPACT_MINUTES,
        COUNT_IF(m.CREDIT_ELIGIBLE)                                 AS CREDIT_ELIGIBLE_SUBS
    FROM matched m
    GROUP BY m.INCIDENT_ID
),

value_at_risk AS (
    SELECT
        sv.INCIDENT_ID,
        SUM(ZEROIFNULL(sv.AVG_DAILY_REVENUE_PHP))                   AS DAILY_REVENUE_OF_AFFECTED_PHP,
        COUNT_IF(sv.CONTRACT_TYPE = 'Two year')                     AS LONG_CONTRACT_SUBS,
        MODE(sv.PLAN_ID)                                            AS TOP_PLAN_ID
    FROM sub_value sv
    GROUP BY sv.INCIDENT_ID
)

SELECT
    i.OUTAGE_MONTH,
    i.INCIDENT_ID,
    i.REGION_ID,
    COALESCE(g.REGION_NAME, 'UNKNOWN')                              AS REGION_NAME,
    i.SEVERITY,
    i.ROOT_CAUSE,
    i.DETECTED_AT                                                   AS DETECTED_AT_UTC,
    {{ utc_to_local('i.DETECTED_AT') }}                             AS DETECTED_AT_LOCAL,
    i.RESTORED_AT                                                   AS RESTORED_AT_UTC,
    i.STILL_OPEN,
    i.DURATION_MIN,
    i.AREA_KM2,
    ROUND(ST_Y(i.AREA_CENTROID), 6)                                 AS CENTROID_LAT,
    ROUND(ST_X(i.AREA_CENTROID), 6)                                 AS CENTROID_LON,
    ZEROIFNULL(pop.POPULATION_IN_AREA)                              AS POPULATION_IN_AREA,
    ZEROIFNULL(pop.HOUSEHOLDS_IN_AREA)                              AS HOUSEHOLDS_IN_AREA,
    ZEROIFNULL(pop.ZIPS_IN_AREA)                                    AS ZIPS_IN_AREA,
    ZEROIFNULL(pi.GEO_SUBSCRIBERS)                                  AS GEO_SUBSCRIBERS,
    ZEROIFNULL(pi.RECORDED_SUBSCRIBERS)                             AS RECORDED_SUBSCRIBERS,
    ZEROIFNULL(pi.IN_BOTH)                                          AS IN_BOTH,
    ZEROIFNULL(pi.GEO_ONLY)                                         AS GEO_ONLY,
    ZEROIFNULL(pi.RECORDED_ONLY)                                    AS RECORDED_ONLY,
    ROUND(DIV0(pi.IN_BOTH, pi.GEO_SUBSCRIBERS), 4)                  AS RECORD_RECALL,
    ROUND(DIV0(pi.IN_BOTH, pi.RECORDED_SUBSCRIBERS), 4)             AS RECORD_PRECISION,
    ZEROIFNULL(pi.FTTH_SUBS_IN_AREA)                                AS FTTH_SUBS_IN_AREA,
    ZEROIFNULL(pi.RECORDED_IMPACT_MINUTES)                          AS RECORDED_IMPACT_MINUTES,
    ZEROIFNULL(pi.CREDIT_ELIGIBLE_SUBS)                             AS CREDIT_ELIGIBLE_SUBS,
    ZEROIFNULL(cs.CELLS_IN_AREA)                                    AS CELLS_IN_AREA,
    ZEROIFNULL(cs.CELLS_5G_IN_AREA)                                 AS CELLS_5G_IN_AREA,
    ZEROIFNULL(cs.ELEMENTS_IN_AREA)                                 AS ELEMENTS_IN_AREA,
    cs.MAIN_VENDOR,
    ZEROIFNULL(al.ALARMS_DURING)                                    AS ALARMS_DURING,
    ZEROIFNULL(al.CRITICAL_ALARMS)                                  AS CRITICAL_ALARMS,
    ZEROIFNULL(al.ALARMS_LINKED)                                    AS ALARMS_LINKED_TO_INCIDENT,
    al.TOP_ALARM_CODE,
    DATEDIFF('minute', al.FIRST_ALARM_AT, i.DETECTED_AT)            AS ALARM_LEAD_MIN,
    ARRAY_SIZE(ARRAY_INTERSECTION(cs.CELL_IDS, al.ALARM_CELL_IDS))  AS CELLS_WITH_ALARMS,
    ZEROIFNULL(td.TICKETS_DURING)                                   AS TICKETS_DURING,
    ZEROIFNULL(td.NETWORK_TICKETS_DURING)                           AS NETWORK_TICKETS_DURING,
    ZEROIFNULL(td.SUBS_WHO_CALLED)                                  AS SUBS_WHO_CALLED,
    ROUND(DIV0(td.SUBS_WHO_CALLED, pi.GEO_SUBSCRIBERS + pi.RECORDED_ONLY), 4) AS CALL_IN_RATE,
    td.TOP_TICKET_CHANNEL,
    ZEROIFNULL(cr.ACCOUNTS_CREDITED)                                AS ACCOUNTS_CREDITED,
    ZEROIFNULL(cr.CREDIT_AMOUNT_PHP)                                AS CREDIT_AMOUNT_PHP,
    ROUND(ZEROIFNULL(va.DAILY_REVENUE_OF_AFFECTED_PHP) * i.DURATION_MIN / 1440, 2) AS REVENUE_AT_RISK_PHP,
    ZEROIFNULL(va.LONG_CONTRACT_SUBS)                               AS LONG_CONTRACT_SUBS,
    va.TOP_PLAN_ID,
    ZEROIFNULL(hi.PRIOR_INCIDENTS_12M)                              AS PRIOR_INCIDENTS_12M,
    ZEROIFNULL(hi.PRIOR_SAME_CAUSE_12M)                             AS PRIOR_SAME_CAUSE_12M,
    hi.LAST_PRIOR_INCIDENT_AT,
    hi.LAST_PRIOR_ROOT_CAUSE,
    DATEDIFF('day', hi.LAST_PRIOR_INCIDENT_AT, i.DETECTED_AT)       AS DAYS_SINCE_PRIOR_INCIDENT,
    ROUND(hi.PRIOR_AVG_DURATION_MIN, 1)                             AS PRIOR_AVG_DURATION_MIN,
    hi.PRIOR_SEVERITIES,
    ZEROIFNULL(cc.CONCURRENT_INCIDENTS_IN_REGION)                   AS CONCURRENT_INCIDENTS_IN_REGION,
    IFF(cc.FIRST_CONCURRENT_DETECTED_AT < i.DETECTED_AT, TRUE, FALSE) AS FOLLOWS_OTHER_INCIDENT,
    rm.REGION_INCIDENTS_IN_MONTH,
    rm.REGION_OUTAGE_MIN_IN_MONTH,
    rm.REGION_MEDIAN_DURATION_MIN,
    ROUND(DIV0(i.DURATION_MIN, rm.REGION_MEDIAN_DURATION_MIN), 3)   AS DURATION_VS_REGION_MEDIAN,
    IFF(hi.PRIOR_SAME_CAUSE_12M >= 2, TRUE, FALSE)                  AS IS_RECURRING_HOTSPOT,
    CASE
        WHEN ZEROIFNULL(pi.GEO_SUBSCRIBERS) = 0 AND ZEROIFNULL(pi.RECORDED_SUBSCRIBERS) = 0 THEN 'NO_SUBSCRIBERS'
        WHEN DIV0(pi.GEO_ONLY, pi.GEO_SUBSCRIBERS) > 0.2            THEN 'UNDER_RECORDED'
        WHEN DIV0(pi.RECORDED_ONLY, pi.RECORDED_SUBSCRIBERS) > 0.2  THEN 'OVER_RECORDED'
        ELSE 'CONSISTENT'
    END                                                             AS RECORD_QUALITY,
    IFF(pi.CREDIT_ELIGIBLE_SUBS > 0 AND ZEROIFNULL(cr.ACCOUNTS_CREDITED) = 0, TRUE, FALSE) AS CREDITS_MISSING,
    RANK() OVER (PARTITION BY i.OUTAGE_MONTH, i.REGION_ID
                 ORDER BY i.DURATION_MIN * ZEROIFNULL(pi.GEO_SUBSCRIBERS) DESC)   AS IMPACT_RANK_IN_REGION,
    CURRENT_TIMESTAMP()                                             AS LOADED_AT
FROM incidents i
LEFT JOIN per_incident pi     ON pi.INCIDENT_ID = i.INCIDENT_ID
LEFT JOIN cell_summary cs     ON cs.INCIDENT_ID = i.INCIDENT_ID
LEFT JOIN alarms al           ON al.INCIDENT_ID = i.INCIDENT_ID
LEFT JOIN tickets_during td   ON td.INCIDENT_ID = i.INCIDENT_ID
LEFT JOIN credits cr          ON cr.INCIDENT_ID = i.INCIDENT_ID
LEFT JOIN value_at_risk va    ON va.INCIDENT_ID = i.INCIDENT_ID
LEFT JOIN population pop      ON pop.INCIDENT_ID = i.INCIDENT_ID
LEFT JOIN history hi          ON hi.INCIDENT_ID = i.INCIDENT_ID
LEFT JOIN concurrent cc       ON cc.INCIDENT_ID = i.INCIDENT_ID
LEFT JOIN region_month rm     ON rm.REGION_ID = i.REGION_ID AND rm.OUTAGE_MONTH = i.OUTAGE_MONTH
LEFT JOIN {{ ref('slv_region') }} g
       ON g.REGION_ID = i.REGION_ID
