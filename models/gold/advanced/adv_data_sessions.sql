{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['session_date'],
    on_schema_change = 'sync_all_columns',
    transient = true,
    cluster_by = ['session_date', 'home_region_name'],
    tags = ['gold', 'advanced', 'h06', 'usage', 'sessionization'],
    query_tag = 'dbt_telco_adv_h06'
  )
}}
{#-
  adv_data_sessions  -  Data sessionization ("visits")
  ------------------------------------------------------------------------------------------
  Ported from x_h06_data_sessionization (PIPE_X.SP_X_H06_DATA_SESSIONIZATION).

  Groups each day's data records into visits per subscriber. A new visit starts after
  30 idle minutes (CONDITIONAL_TRUE_EVENT). Every visit is enriched with the voice calls,
  SMS and mediation events that happened inside it, the cells and radio types it used,
  cell congestion at the time (15-minute KPIs aligned with TIME_SLICE), the subscriber's
  plan and device, and how the visit compares with the subscriber's 30-day baseline.

  Grain     : one row per (session_date, subscription_id, visit_no). All times Manila local.
  Window    : full refresh builds the last var('adv_daily_days', 7) days present in
              slv_cdr_data (per-event model, sized for an X-Small warehouse); incremental runs
              rebuild the last var('lookback_days') loaded days and every newer day.
              Backfill more days with --vars '{adv_daily_days: 30}' --full-refresh.
  Changed vs original: one RUN_DAY -> rolling day window (visits partitioned per day);
              CDR timestamps come from silver (UTC-correct, Manila local), xDR events are
              anchored on the parsed session start instead of RECEIVED_AT and matched by
              subscription id instead of MSISDN; 15-min KPI intervals (stored UTC) are
              converted to Manila before slice matching; MB = bytes / 2^20 (silver rule).
  Snowflake : CONDITIONAL_TRUE_EVENT, TIME_SLICE, CONVERT_TIMEZONE, LATERAL FLATTEN over
              VARIANT/ARRAY, APPROX_PERCENTILE, MODE, ARRAY_AGG DISTINCT, ARRAY_SIZE,
              BOOLOR_AGG, IFF, DIV0, EQUAL_NULL, QUALIFY, ZEROIFNULL, PERCENT_RANK, HASH.
  Sources (11): slv_cdr_data, slv_cdr_voice, slv_cdr_sms, slv_xdr_session,
              network.cell_performance_15min (bronze), slv_cell_site, slv_network_element,
              slv_subscription, slv_plan, slv_device, fct_subscriber_usage_daily.
-#}

WITH params AS (
    SELECT
        {{ f3_day_lower_bound('session_date', ref('slv_cdr_data'), 'event_date') }} AS FIRST_DAY,
        30                                                          AS IDLE_GAP_MINUTES
),

/* ---------- 1. data records of the window ---------- */
data_records AS (
    SELECT
        d.EVENT_DATE                                                AS SESSION_DATE,
        d.SESSION_ID,
        d.SUBSCRIPTION_ID,
        d.CELL_ID,
        d.SESSION_START_LOCAL                                       AS SESSION_START,
        DATEADD('second', d.DURATION_SEC, d.SESSION_START_LOCAL)    AS SESSION_END,
        d.BYTES_UP,
        d.BYTES_DOWN,
        d.BYTES_TOTAL,
        LOWER(d.APN)                                                AS APN,
        d.RAT,
        d.IS_ROAMING,
        TIME_SLICE(d.SESSION_START_LOCAL, 15, 'MINUTE')             AS SLICE_15M,
        d.DURATION_SEC                                              AS RECORD_SECONDS
    FROM {{ ref('slv_cdr_data') }} d
    CROSS JOIN params p
    WHERE d.EVENT_DATE >= p.FIRST_DAY
),

gaps AS (
    SELECT
        r.*,
        LAG(r.SESSION_END) OVER (PARTITION BY r.SUBSCRIPTION_ID, r.SESSION_DATE ORDER BY r.SESSION_START, r.SESSION_ID) AS PREV_END,
        LAG(r.CELL_ID)     OVER (PARTITION BY r.SUBSCRIPTION_ID, r.SESSION_DATE ORDER BY r.SESSION_START, r.SESSION_ID) AS PREV_CELL_ID,
        LAG(r.RAT)         OVER (PARTITION BY r.SUBSCRIPTION_ID, r.SESSION_DATE ORDER BY r.SESSION_START, r.SESSION_ID) AS PREV_RAT
    FROM data_records r
),

visit_numbered AS (
    SELECT
        g.*,
        DATEDIFF('minute', g.PREV_END, g.SESSION_START)             AS GAP_MINUTES,
        CONDITIONAL_TRUE_EVENT(
            g.PREV_END IS NULL
            OR DATEDIFF('minute', g.PREV_END, g.SESSION_START) > 30
        ) OVER (PARTITION BY g.SUBSCRIPTION_ID, g.SESSION_DATE ORDER BY g.SESSION_START, g.SESSION_ID) AS VISIT_NO,
        IFF(g.PREV_CELL_ID IS NOT NULL AND g.PREV_CELL_ID <> g.CELL_ID, 1, 0) AS IS_HANDOVER,
        IFF(g.PREV_RAT IS NOT NULL AND g.PREV_RAT <> g.RAT, 1, 0)   AS IS_RAT_CHANGE
    FROM gaps g
),

visits AS (
    SELECT
        v.SESSION_DATE,
        v.SUBSCRIPTION_ID,
        v.VISIT_NO,
        MIN(v.SESSION_START)                                        AS VISIT_START,
        MAX(v.SESSION_END)                                          AS VISIT_END,
        COUNT(*)                                                    AS RECORDS,
        SUM(v.BYTES_UP)                                             AS BYTES_UP,
        SUM(v.BYTES_DOWN)                                           AS BYTES_DOWN,
        ROUND(SUM(v.BYTES_TOTAL) / 1048576, 3)                      AS DATA_MB,
        SUM(v.RECORD_SECONDS)                                       AS ACTIVE_SECONDS,
        SUM(v.IS_HANDOVER)                                          AS HANDOVERS,
        SUM(v.IS_RAT_CHANGE)                                        AS RAT_CHANGES,
        COUNT(DISTINCT v.CELL_ID)                                   AS CELLS,
        MODE(v.CELL_ID)                                             AS MAIN_CELL_ID,
        ARRAY_AGG(DISTINCT v.CELL_ID) WITHIN GROUP (ORDER BY v.CELL_ID) AS CELL_LIST,
        ARRAY_AGG(DISTINCT v.RAT)                                   AS RAT_LIST,
        MODE(v.RAT)                                                 AS MAIN_RAT,
        MODE(v.APN)                                                 AS MAIN_APN,
        COUNT_IF(v.APN = 'corp.vpn')                                AS VPN_RECORDS,
        BOOLOR_AGG(v.IS_ROAMING)                                    AS ANY_ROAMING,
        MAX(v.GAP_MINUTES)                                          AS MAX_GAP_INSIDE_MIN,
        ARRAY_AGG(DISTINCT v.SLICE_15M)                             AS SLICES
    FROM visit_numbered v
    GROUP BY v.SESSION_DATE, v.SUBSCRIPTION_ID, v.VISIT_NO
),

/* ---------- 2. other activity inside each visit ---------- */
voice_day AS (
    SELECT
        c.EVENT_DATE,
        c.SUBSCRIPTION_ID,
        c.CALL_START_LOCAL,
        c.DURATION_SEC,
        c.CALL_TYPE,
        c.CHARGE_PHP
    FROM {{ ref('slv_cdr_voice') }} c
    CROSS JOIN params p
    WHERE c.EVENT_DATE >= p.FIRST_DAY
),

sms_day AS (
    SELECT
        s.EVENT_DATE,
        s.SUBSCRIPTION_ID,
        s.SENT_AT_LOCAL,
        s.SMS_TYPE,
        s.CHARGE_PHP
    FROM {{ ref('slv_cdr_sms') }} s
    CROSS JOIN params p
    WHERE s.EVENT_DATE >= p.FIRST_DAY
),

voice_in_visit AS (
    SELECT
        v.SESSION_DATE,
        v.SUBSCRIPTION_ID,
        v.VISIT_NO,
        COUNT(*)                                                    AS CALLS,
        SUM(c.DURATION_SEC)                                         AS CALL_SECONDS,
        COUNT_IF(c.CALL_TYPE = 'MO')                                AS MO_CALLS,
        SUM(c.CHARGE_PHP)                                           AS CALL_CHARGE_PHP
    FROM visits v
    JOIN voice_day c
      ON c.SUBSCRIPTION_ID = v.SUBSCRIPTION_ID
     AND c.EVENT_DATE = v.SESSION_DATE
     AND c.CALL_START_LOCAL BETWEEN v.VISIT_START AND v.VISIT_END
    GROUP BY v.SESSION_DATE, v.SUBSCRIPTION_ID, v.VISIT_NO
),

sms_in_visit AS (
    SELECT
        v.SESSION_DATE,
        v.SUBSCRIPTION_ID,
        v.VISIT_NO,
        COUNT(*)                                                    AS SMS,
        COUNT_IF(s.SMS_TYPE = 'PREMIUM')                            AS PREMIUM_SMS,
        SUM(s.CHARGE_PHP)                                           AS SMS_CHARGE_PHP
    FROM visits v
    JOIN sms_day s
      ON s.SUBSCRIPTION_ID = v.SUBSCRIPTION_ID
     AND s.EVENT_DATE = v.SESSION_DATE
     AND s.SENT_AT_LOCAL BETWEEN v.VISIT_START AND v.VISIT_END
    GROUP BY v.SESSION_DATE, v.SUBSCRIPTION_ID, v.VISIT_NO
),

/* mediation records: unpack the event list, anchor each event on the session start */
xdr_events AS (
    SELECT
        x.EVENT_DATE,
        x.RECORD_ID,
        x.SOURCE_SYSTEM,
        x.SUBSCRIPTION_ID,
        x.CELL_ID,
        x.CHARGE_PHP,
        x.QCI,
        DATEADD('millisecond', e.value:ms::NUMBER, x.SESSION_START_LOCAL) AS EVENT_AT_LOCAL,
        e.value:t::STRING                                           AS EVENT_TYPE
    FROM {{ ref('slv_xdr_session') }} x
    CROSS JOIN params p,
    LATERAL FLATTEN(INPUT => x.EVENTS, OUTER => FALSE) e
    WHERE x.EVENT_DATE >= p.FIRST_DAY
),

xdr_in_visit AS (
    SELECT
        v.SESSION_DATE,
        v.SUBSCRIPTION_ID,
        v.VISIT_NO,
        COUNT(*)                                                    AS XDR_EVENTS,
        COUNT_IF(xe.EVENT_TYPE = 'HANDOVER')                        AS XDR_HANDOVERS,
        COUNT_IF(xe.EVENT_TYPE = 'DETACH')                          AS XDR_DETACHES,
        COUNT(DISTINCT xe.RECORD_ID)                                AS XDR_RECORDS,
        MAX(xe.QCI)                                                 AS MAX_QCI
    FROM visits v
    JOIN xdr_events xe
      ON xe.SUBSCRIPTION_ID = v.SUBSCRIPTION_ID
     AND xe.EVENT_DATE = v.SESSION_DATE
     AND xe.EVENT_AT_LOCAL BETWEEN v.VISIT_START AND v.VISIT_END
    GROUP BY v.SESSION_DATE, v.SUBSCRIPTION_ID, v.VISIT_NO
),

/* ---------- 3. network conditions while the visit was running ---------- */
cell_kpi AS (
    SELECT
        k.CELL_ID,
        {{ utc_to_local('k.INTERVAL_START') }}                      AS INTERVAL_START_LOCAL,
        k.PRB_UTILIZATION_PCT,
        IFF(EQUAL_NULL(k.LATENCY_MS, 'NaN'::FLOAT) OR k.LATENCY_MS < 0, NULL, k.LATENCY_MS) AS LATENCY_MS,
        k.AVG_THROUGHPUT_MBPS,
        k.DROP_CALL_RATE * 100                                      AS DROP_RATE_PCT
    FROM {{ source('network', 'cell_performance_15min') }} k
    CROSS JOIN params p
    WHERE k.INTERVAL_START >= DATEADD('day', -1, p.FIRST_DAY)::TIMESTAMP_NTZ
    QUALIFY ROW_NUMBER() OVER (PARTITION BY k.CELL_ID, k.INTERVAL_START ORDER BY k.ACTIVE_USERS DESC) = 1
),

visit_slices AS (
    SELECT
        v.SESSION_DATE,
        v.SUBSCRIPTION_ID,
        v.VISIT_NO,
        v.MAIN_CELL_ID,
        sl.value::TIMESTAMP_NTZ                                     AS SLICE_15M
    FROM visits v,
    LATERAL FLATTEN(INPUT => v.SLICES) sl
),

congestion AS (
    SELECT
        vs.SESSION_DATE,
        vs.SUBSCRIPTION_ID,
        vs.VISIT_NO,
        AVG(k.PRB_UTILIZATION_PCT)                                  AS AVG_PRB_PCT,
        MAX(k.PRB_UTILIZATION_PCT)                                  AS MAX_PRB_PCT,
        AVG(k.LATENCY_MS)                                           AS AVG_LATENCY_MS,
        MIN(k.AVG_THROUGHPUT_MBPS)                                  AS MIN_THROUGHPUT_MBPS,
        MAX(k.DROP_RATE_PCT)                                        AS MAX_DROP_RATE_PCT,
        COUNT_IF(k.PRB_UTILIZATION_PCT >= 85)                       AS CONGESTED_SLICES,
        COUNT(*)                                                    AS SLICES_WITH_KPI
    FROM visit_slices vs
    JOIN cell_kpi k
      ON k.CELL_ID = vs.MAIN_CELL_ID
     AND k.INTERVAL_START_LOCAL = vs.SLICE_15M
    GROUP BY vs.SESSION_DATE, vs.SUBSCRIPTION_ID, vs.VISIT_NO
),

cell_info AS (
    SELECT
        cs.CELL_ID,
        cs.SITE_NAME,
        cs.RADIO_TYPE,
        cs.REGION_ID,
        COALESCE(cs.REGION_NAME, 'UNKNOWN')                         AS REGION_NAME,
        MODE(ne.VENDOR)                                             AS VENDOR,
        COUNT(ne.ELEMENT_ID)                                        AS ELEMENTS_ON_CELL
    FROM {{ ref('slv_cell_site') }} cs
    LEFT JOIN {{ ref('slv_network_element') }} ne
           ON ne.CELL_ID = cs.CELL_ID
    GROUP BY cs.CELL_ID, cs.SITE_NAME, cs.RADIO_TYPE, cs.REGION_ID, cs.REGION_NAME
),

/* ---------- 4. subscriber context and 30-day baseline ---------- */
subscriber AS (
    SELECT
        s.SUBSCRIPTION_ID,
        s.PLAN_ID,
        s.HOME_CELL_ID,
        pc.PLAN_NAME,
        pc.CONTRACT_TYPE,
        pc.DATA_CAP_GB,
        pc.SPEED_MBPS                                               AS PLAN_SPEED_MBPS,
        dc.MANUFACTURER,
        dc.DEVICE_TYPE,
        COALESCE(dc.SUPPORTS_5G, FALSE)                             AS DEVICE_5G
    FROM {{ ref('slv_subscription') }} s
    LEFT JOIN {{ ref('slv_plan') }} pc
           ON pc.PLAN_ID = s.PLAN_ID
    LEFT JOIN {{ ref('slv_device') }} dc
           ON dc.DEVICE_ID = s.DEVICE_ID
    WHERE s.SUBSCRIPTION_ID IN (SELECT SUBSCRIPTION_ID FROM visits)
),

sub_days AS (
    SELECT DISTINCT SESSION_DATE, SUBSCRIPTION_ID FROM visits
),

baseline AS (
    SELECT
        sd.SESSION_DATE,
        sd.SUBSCRIPTION_ID,
        AVG(u.DATA_MB)                                              AS AVG_DAILY_MB_30D,
        APPROX_PERCENTILE(u.DATA_MB, 0.9)                           AS P90_DAILY_MB_30D,
        AVG(u.DATA_SESSIONS)                                        AS AVG_SESSIONS_30D,
        COUNT(*)                                                    AS ACTIVE_DAYS_30D
    FROM sub_days sd
    JOIN {{ ref('fct_subscriber_usage_daily') }} u
      ON u.SUBSCRIPTION_ID = sd.SUBSCRIPTION_ID
     AND u.EVENT_DATE >= DATEADD('day', -30, sd.SESSION_DATE)
     AND u.EVENT_DATE <  sd.SESSION_DATE
    GROUP BY sd.SESSION_DATE, sd.SUBSCRIPTION_ID
),

day_totals AS (
    SELECT
        SESSION_DATE,
        SUBSCRIPTION_ID,
        SUM(DATA_MB)                                                AS DAY_DATA_MB,
        COUNT(*)                                                    AS DAY_VISITS
    FROM visits
    GROUP BY SESSION_DATE, SUBSCRIPTION_ID
),

/* ---------- 5. assemble ---------- */
assembled AS (
    SELECT
        v.SESSION_DATE,
        v.SUBSCRIPTION_ID,
        v.VISIT_NO,
        v.VISIT_START,
        v.VISIT_END,
        DATEDIFF('minute', v.VISIT_START, v.VISIT_END)              AS VISIT_MINUTES,
        HOUR(v.VISIT_START)                                         AS START_HOUR,
        CASE
            WHEN HOUR(v.VISIT_START) BETWEEN 0 AND 5   THEN 'NIGHT'
            WHEN HOUR(v.VISIT_START) BETWEEN 6 AND 11  THEN 'MORNING'
            WHEN HOUR(v.VISIT_START) BETWEEN 12 AND 17 THEN 'AFTERNOON'
            ELSE 'EVENING'
        END                                                         AS DAY_PART,
        v.RECORDS,
        v.DATA_MB,
        v.BYTES_UP,
        v.BYTES_DOWN,
        ROUND(DIV0(v.BYTES_UP, v.BYTES_DOWN), 4)                    AS UP_DOWN_RATIO,
        v.ACTIVE_SECONDS,
        ROUND(DIV0(v.BYTES_DOWN * 8 / 1e6, v.ACTIVE_SECONDS), 3)    AS EFFECTIVE_DOWN_MBPS,
        v.HANDOVERS,
        v.RAT_CHANGES,
        v.CELLS,
        v.MAIN_CELL_ID,
        v.CELL_LIST,
        ARRAY_SIZE(v.RAT_LIST)                                      AS RAT_COUNT,
        v.MAIN_RAT,
        v.MAIN_APN,
        v.VPN_RECORDS,
        v.ANY_ROAMING,
        v.MAX_GAP_INSIDE_MIN,
        ci.SITE_NAME                                                AS MAIN_SITE_NAME,
        ci.RADIO_TYPE                                               AS MAIN_CELL_RADIO,
        ci.VENDOR                                                   AS MAIN_CELL_VENDOR,
        ci.REGION_NAME                                              AS VISIT_REGION_NAME,
        COALESCE(hci.REGION_NAME, 'UNKNOWN')                        AS HOME_REGION_NAME,
        IFF(v.MAIN_CELL_ID = sb.HOME_CELL_ID, TRUE, FALSE)          AS AT_HOME_CELL,
        ZEROIFNULL(vo.CALLS)                                        AS CALLS_IN_VISIT,
        ZEROIFNULL(vo.CALL_SECONDS)                                 AS CALL_SECONDS_IN_VISIT,
        ZEROIFNULL(vo.MO_CALLS)                                     AS MO_CALLS_IN_VISIT,
        ZEROIFNULL(vo.CALL_CHARGE_PHP)                              AS CALL_CHARGE_PHP,
        ZEROIFNULL(sm.SMS)                                          AS SMS_IN_VISIT,
        ZEROIFNULL(sm.PREMIUM_SMS)                                  AS PREMIUM_SMS_IN_VISIT,
        ZEROIFNULL(sm.SMS_CHARGE_PHP)                               AS SMS_CHARGE_PHP,
        ZEROIFNULL(xv.XDR_EVENTS)                                   AS XDR_EVENTS,
        ZEROIFNULL(xv.XDR_HANDOVERS)                                AS XDR_HANDOVERS,
        ZEROIFNULL(xv.XDR_DETACHES)                                 AS XDR_DETACHES,
        xv.MAX_QCI,
        ROUND(cg.AVG_PRB_PCT, 2)                                    AS AVG_PRB_PCT,
        ROUND(cg.MAX_PRB_PCT, 2)                                    AS MAX_PRB_PCT,
        ROUND(cg.AVG_LATENCY_MS, 2)                                 AS AVG_LATENCY_MS,
        ROUND(cg.MIN_THROUGHPUT_MBPS, 2)                            AS MIN_THROUGHPUT_MBPS,
        ROUND(cg.MAX_DROP_RATE_PCT, 4)                              AS MAX_DROP_RATE_PCT,
        ZEROIFNULL(cg.CONGESTED_SLICES)                             AS CONGESTED_SLICES,
        sb.PLAN_ID,
        sb.PLAN_NAME,
        sb.CONTRACT_TYPE,
        sb.DATA_CAP_GB,
        sb.PLAN_SPEED_MBPS,
        sb.MANUFACTURER                                             AS DEVICE_MANUFACTURER,
        sb.DEVICE_TYPE,
        sb.DEVICE_5G,
        IFF(v.MAIN_RAT = '5G' AND NOT COALESCE(sb.DEVICE_5G, FALSE), TRUE, FALSE) AS RAT_DEVICE_MISMATCH,
        ROUND(bl.AVG_DAILY_MB_30D, 3)                               AS AVG_DAILY_MB_30D,
        ROUND(bl.P90_DAILY_MB_30D, 3)                               AS P90_DAILY_MB_30D,
        ZEROIFNULL(bl.ACTIVE_DAYS_30D)                              AS ACTIVE_DAYS_30D,
        dt.DAY_DATA_MB,
        dt.DAY_VISITS,
        ROUND(DIV0(v.DATA_MB, dt.DAY_DATA_MB), 4)                   AS SHARE_OF_DAY_MB
    FROM visits v
    LEFT JOIN cell_info ci       ON ci.CELL_ID = v.MAIN_CELL_ID
    LEFT JOIN subscriber sb      ON sb.SUBSCRIPTION_ID = v.SUBSCRIPTION_ID
    LEFT JOIN cell_info hci      ON hci.CELL_ID = sb.HOME_CELL_ID
    LEFT JOIN voice_in_visit vo  ON vo.SESSION_DATE = v.SESSION_DATE AND vo.SUBSCRIPTION_ID = v.SUBSCRIPTION_ID AND vo.VISIT_NO = v.VISIT_NO
    LEFT JOIN sms_in_visit sm    ON sm.SESSION_DATE = v.SESSION_DATE AND sm.SUBSCRIPTION_ID = v.SUBSCRIPTION_ID AND sm.VISIT_NO = v.VISIT_NO
    LEFT JOIN xdr_in_visit xv    ON xv.SESSION_DATE = v.SESSION_DATE AND xv.SUBSCRIPTION_ID = v.SUBSCRIPTION_ID AND xv.VISIT_NO = v.VISIT_NO
    LEFT JOIN congestion cg      ON cg.SESSION_DATE = v.SESSION_DATE AND cg.SUBSCRIPTION_ID = v.SUBSCRIPTION_ID AND cg.VISIT_NO = v.VISIT_NO
    LEFT JOIN baseline bl        ON bl.SESSION_DATE = v.SESSION_DATE AND bl.SUBSCRIPTION_ID = v.SUBSCRIPTION_ID
    LEFT JOIN day_totals dt      ON dt.SESSION_DATE = v.SESSION_DATE AND dt.SUBSCRIPTION_ID = v.SUBSCRIPTION_ID
)

SELECT
    HASH(a.SESSION_DATE, a.SUBSCRIPTION_ID, a.VISIT_NO)             AS VISIT_KEY,
    a.*,
    CASE
        WHEN a.VPN_RECORDS > 0                                      THEN 'WORK_VPN'
        WHEN a.CALLS_IN_VISIT > 0 AND a.DATA_MB < 5                 THEN 'VOICE_WITH_LIGHT_DATA'
        WHEN a.DATA_MB >= 500                                       THEN 'HEAVY_STREAMING'
        WHEN a.CELLS >= 4 OR a.HANDOVERS >= 4                       THEN 'ON_THE_MOVE'
        WHEN a.VISIT_MINUTES <= 2                                   THEN 'PING'
        ELSE 'BROWSING'
    END                                                             AS VISIT_PROFILE,
    CASE
        WHEN a.CONGESTED_SLICES > 0 AND a.EFFECTIVE_DOWN_MBPS < 1   THEN 'POOR'
        WHEN a.AVG_LATENCY_MS > 80 OR a.CONGESTED_SLICES > 0        THEN 'DEGRADED'
        WHEN a.AVG_PRB_PCT IS NULL                                  THEN 'NO_KPI'
        ELSE 'GOOD'
    END                                                             AS EXPERIENCE,
    IFF(a.DAY_DATA_MB > COALESCE(a.P90_DAILY_MB_30D, 0) AND a.ACTIVE_DAYS_30D >= 5, TRUE, FALSE) AS DAY_ABOVE_P90,
    ROW_NUMBER() OVER (PARTITION BY a.SESSION_DATE, a.SUBSCRIPTION_ID ORDER BY a.DATA_MB DESC, a.VISIT_NO) AS VISIT_RANK_BY_MB,
    PERCENT_RANK() OVER (PARTITION BY a.SESSION_DATE, a.HOME_REGION_NAME ORDER BY a.DATA_MB)              AS MB_PERCENTILE_IN_REGION,
    CURRENT_TIMESTAMP()                                             AS LOADED_AT
FROM assembled a
