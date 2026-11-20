{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['snapshot_date'],
    on_schema_change = 'sync_all_columns',
    transient = true,
    cluster_by = ['snapshot_date', 'root_id', 'depth'],
    tags = ['gold', 'advanced', 'h09', 'ftth', 'network'],
    query_tag = 'dbt_telco_adv_h09'
  )
}}
{#-
  adv_ftth_hierarchy_rollup  -  FTTH hierarchy roll-up (daily node snapshot)
  ------------------------------------------------------------------------------------------
  Ported from x_h09_ftth_hierarchy_rollup (PIPE_X.SP_X_H09_FTTH_HIERARCHY_ROLLUP).

  Walks the fibre network tree (OLT -> CABINET -> SPLITTER -> PORT -> LINE) with a recursive
  CTE and rolls up, to every ancestor node, the active services under it, ONT optical health
  from the last 3 days, customer tickets and field work, outages in the node's region, and
  month-to-date revenue of the subscribers it serves. Each node gets a capacity utilisation,
  a distance to its parent (geography) and a health score.

  Grain     : snapshot_date x node_id. snapshot_date = latest business date in silver
              revenue; each run appends/replaces that day's snapshot, so history accrues.
  Changed vs original: RUN_DAY param -> data-driven as-of date; ONT optical health comes from
              silver slv_ont_health_daily (poll-weighted), only reboot detection still reads
              the raw 15-minute telemetry for the 3-day window; 30-day windows use silver
              Manila business dates.
  Snowflake : WITH RECURSIVE, ARRAY_CONSTRUCT / ARRAY_APPEND / ARRAY_TO_STRING, LATERAL
              FLATTEN, ST_DISTANCE / ST_X / ST_Y on GEOGRAPHY, MEDIAN, EQUAL_NULL, IFF, DIV0,
              ZEROIFNULL, QUALIFY, PERCENT_RANK.
  Sources (13): network.ftth_topology (bronze), network.ont_telemetry (bronze, reboots),
              slv_service_instance, slv_subscription, slv_plan, slv_ont_health_daily,
              slv_ticket, slv_work_order, slv_daily_revenue, slv_outage_incident,
              slv_outage_impact, slv_region.
-#}

WITH RECURSIVE params AS (
    SELECT
        RUN_DAY,
        DATEADD('day', 1, RUN_DAY)                                  AS NEXT_DAY,
        DATEADD('day', -2, RUN_DAY)                                 AS TELEMETRY_START,
        DATE_TRUNC('month', RUN_DAY)                                AS MONTH_START,
        -27.0                                                       AS WEAK_RX_DBM,
        -30.0                                                       AS CRITICAL_RX_DBM,
        70.0                                                        AS HOT_ONT_C
    FROM (SELECT MAX(BUSINESS_DATE) AS RUN_DAY FROM {{ ref('slv_daily_revenue') }})
),

/* ---------- 1. walk the tree from every root ---------- */
tree (NODE_ID, PARENT_NODE_ID, ROOT_ID, DEPTH, PATH, TYPE_PATH) AS (
    SELECT
        t.NODE_ID,
        t.PARENT_NODE_ID,
        t.NODE_ID,
        0,
        ARRAY_CONSTRUCT(t.NODE_ID),
        ARRAY_CONSTRUCT(t.NODE_TYPE)
    FROM {{ source('network', 'ftth_topology') }} t
    WHERE t.PARENT_NODE_ID IS NULL
    UNION ALL
    SELECT
        c.NODE_ID,
        c.PARENT_NODE_ID,
        tr.ROOT_ID,
        tr.DEPTH + 1,
        ARRAY_APPEND(tr.PATH, c.NODE_ID),
        ARRAY_APPEND(tr.TYPE_PATH, c.NODE_TYPE)
    FROM {{ source('network', 'ftth_topology') }} c
    JOIN tree tr
      ON c.PARENT_NODE_ID = tr.NODE_ID
    WHERE tr.DEPTH < 10
),

nodes AS (
    SELECT
        tr.NODE_ID,
        tr.PARENT_NODE_ID,
        tr.ROOT_ID,
        tr.DEPTH,
        tr.PATH,
        tr.TYPE_PATH,
        n.NODE_TYPE,
        n.NODE_NAME,
        n.REGION_ID,
        n.CAPACITY,
        n.LOCATION,
        pn.LOCATION                                                 AS PARENT_LOCATION,
        rn.NODE_NAME                                                AS ROOT_NAME
    FROM tree tr
    JOIN {{ source('network', 'ftth_topology') }} n
      ON n.NODE_ID = tr.NODE_ID
    LEFT JOIN {{ source('network', 'ftth_topology') }} pn
           ON pn.NODE_ID = tr.PARENT_NODE_ID
    LEFT JOIN {{ source('network', 'ftth_topology') }} rn
           ON rn.NODE_ID = tr.ROOT_ID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tr.NODE_ID ORDER BY tr.DEPTH) = 1
),

/* every node paired with each of its ancestors (itself included) */
ancestry AS (
    SELECT
        n.NODE_ID,
        a.value::NUMBER                                             AS ANCESTOR_ID,
        a.index                                                     AS ANCESTOR_DEPTH
    FROM nodes n,
    LATERAL FLATTEN(INPUT => n.PATH) a
),

/* ---------- 2. services hanging on each node ---------- */
services AS (
    SELECT
        si.SERVICE_INSTANCE_ID,
        si.SUBSCRIPTION_ID,
        si.FTTH_NODE_ID                                             AS NODE_ID,
        si.ONT_SERIAL,
        si.SERVICE_TYPE,
        si.PROVISIONED_AT_UTC,
        si.CITY,
        si.ZIP_CODE,
        s.PLAN_ID,
        s.STATUS                                                    AS SUB_STATUS,
        pc.SPEED_MBPS                                               AS PLAN_SPEED_MBPS
    FROM {{ ref('slv_service_instance') }} si
    CROSS JOIN params p
    LEFT JOIN {{ ref('slv_subscription') }} s
           ON s.SUBSCRIPTION_ID = si.SUBSCRIPTION_ID
    LEFT JOIN {{ ref('slv_plan') }} pc
           ON pc.PLAN_ID = s.PLAN_ID
    WHERE si.FTTH_NODE_ID IS NOT NULL
      AND {{ utc_to_local('si.PROVISIONED_AT_UTC') }} < p.NEXT_DAY
      AND (si.DECOMMISSIONED_AT_UTC IS NULL OR {{ utc_to_local('si.DECOMMISSIONED_AT_UTC') }} >= p.NEXT_DAY)
),

/* ---------- 3. ONT health over the last 3 days ---------- */
telemetry AS (
    SELECT
        h.ONT_SERIAL,
        DIV0(SUM(h.RX_DBM_AVG * h.POLLS), SUM(h.POLLS))             AS AVG_RX_DBM,
        MIN(h.RX_DBM_MIN)                                           AS MIN_RX_DBM,
        DIV0(SUM(h.TX_DBM_AVG * h.POLLS), SUM(h.POLLS))             AS AVG_TX_DBM,
        MAX(h.TEMPERATURE_C_MAX)                                    AS MAX_TEMP_C,
        SUM(h.POLLS)                                                AS POLLS
    FROM {{ ref('slv_ont_health_daily') }} h
    CROSS JOIN params p
    WHERE h.EVENT_DATE >= p.TELEMETRY_START
      AND h.EVENT_DATE <= p.RUN_DAY
    GROUP BY h.ONT_SERIAL
),

reboots AS (
    SELECT
        ONT_SERIAL,
        SUM(IS_REBOOT)                                              AS REBOOTS
    FROM (
        SELECT
            t.ONT_SERIAL,
            IFF(t.UPTIME_SEC < LAG(t.UPTIME_SEC) OVER (PARTITION BY t.ONT_SERIAL ORDER BY t.POLLED_AT), 1, 0) AS IS_REBOOT
        FROM {{ source('network', 'ont_telemetry') }} t
        CROSS JOIN params p
        WHERE t.POLLED_AT >= DATEADD('hour', -8, p.TELEMETRY_START::TIMESTAMP_NTZ)
          AND t.POLLED_AT <  DATEADD('hour', -8, p.NEXT_DAY::TIMESTAMP_NTZ)
          AND NOT EQUAL_NULL(t.OPTICAL_RX_DBM, 'NaN'::FLOAT)
    )
    GROUP BY ONT_SERIAL
),

service_health AS (
    SELECT
        sv.*,
        tl.AVG_RX_DBM,
        tl.MIN_RX_DBM,
        tl.AVG_TX_DBM,
        tl.MAX_TEMP_C,
        ZEROIFNULL(tl.POLLS)                                        AS POLLS,
        IFF(tl.AVG_RX_DBM < p.WEAK_RX_DBM, 1, 0)                    AS IS_WEAK,
        IFF(tl.MIN_RX_DBM < p.CRITICAL_RX_DBM, 1, 0)                AS IS_CRITICAL,
        IFF(tl.MAX_TEMP_C > p.HOT_ONT_C, 1, 0)                      AS IS_HOT,
        IFF(tl.POLLS IS NULL, 1, 0)                                 AS IS_SILENT,
        ZEROIFNULL(rb.REBOOTS)                                      AS REBOOTS
    FROM services sv
    CROSS JOIN params p
    LEFT JOIN telemetry tl
           ON tl.ONT_SERIAL = sv.ONT_SERIAL
    LEFT JOIN reboots rb
           ON rb.ONT_SERIAL = sv.ONT_SERIAL
),

/* ---------- 4. customer pain on each service ---------- */
tickets AS (
    SELECT
        t.SUBSCRIPTION_ID,
        COUNT(*)                                                    AS TICKETS_30D,
        COUNT_IF(t.CATEGORY IN ('NO_INTERNET', 'SLOW_SPEED'))       AS FIBRE_TICKETS_30D,
        COUNT_IF(t.STATUS = 'OPEN')                                 AS OPEN_TICKETS
    FROM {{ ref('slv_ticket') }} t
    CROSS JOIN params p
    WHERE t.OPENED_DATE >= DATEADD('day', -29, p.RUN_DAY)
      AND t.OPENED_DATE <= p.RUN_DAY
    GROUP BY t.SUBSCRIPTION_ID
),

work_orders AS (
    SELECT
        tk.SUBSCRIPTION_ID,
        COUNT(*)                                                    AS WORK_ORDERS_30D,
        COUNT_IF(w.OUTCOME = 'REPLACED_CPE')                        AS CPE_REPLACED_30D,
        AVG(DATEDIFF('hour', w.CREATED_AT_LOCAL, w.COMPLETED_AT_LOCAL)) AS AVG_HOURS_TO_COMPLETE
    FROM {{ ref('slv_work_order') }} w
    CROSS JOIN params p
    JOIN {{ ref('slv_ticket') }} tk
      ON tk.TICKET_ID = w.TICKET_ID
    WHERE w.CREATED_DATE >= DATEADD('day', -29, p.RUN_DAY)
      AND w.CREATED_DATE <= p.RUN_DAY
    GROUP BY tk.SUBSCRIPTION_ID
),

revenue AS (
    SELECT
        r.SUBSCRIPTION_ID,
        SUM(r.REVENUE_PHP)                                          AS REVENUE_MTD_PHP
    FROM {{ ref('slv_daily_revenue') }} r
    CROSS JOIN params p
    WHERE r.BUSINESS_DATE >= p.MONTH_START
      AND r.BUSINESS_DATE <= p.RUN_DAY
    GROUP BY r.SUBSCRIPTION_ID
),

leaf AS (
    SELECT
        sh.NODE_ID,
        COUNT(*)                                                    AS SERVICES,
        COUNT_IF(sh.SUB_STATUS = 'ACTIVE')                          AS ACTIVE_SUBS,
        SUM(sh.IS_WEAK)                                             AS WEAK_ONTS,
        SUM(sh.IS_CRITICAL)                                         AS CRITICAL_ONTS,
        SUM(sh.IS_HOT)                                              AS HOT_ONTS,
        SUM(sh.IS_SILENT)                                           AS SILENT_ONTS,
        SUM(sh.REBOOTS)                                             AS ONT_REBOOTS,
        SUM(sh.AVG_RX_DBM)                                          AS SUM_RX_DBM,
        COUNT(sh.AVG_RX_DBM)                                        AS RX_SAMPLES,
        SUM(ZEROIFNULL(sh.PLAN_SPEED_MBPS))                         AS SOLD_MBPS,
        SUM(ZEROIFNULL(tk.TICKETS_30D))                             AS TICKETS_30D,
        SUM(ZEROIFNULL(tk.FIBRE_TICKETS_30D))                       AS FIBRE_TICKETS_30D,
        SUM(ZEROIFNULL(tk.OPEN_TICKETS))                            AS OPEN_TICKETS,
        SUM(ZEROIFNULL(wo.WORK_ORDERS_30D))                         AS WORK_ORDERS_30D,
        SUM(ZEROIFNULL(wo.CPE_REPLACED_30D))                        AS CPE_REPLACED_30D,
        SUM(ZEROIFNULL(rv.REVENUE_MTD_PHP))                         AS REVENUE_MTD_PHP
    FROM service_health sh
    LEFT JOIN tickets tk     ON tk.SUBSCRIPTION_ID = sh.SUBSCRIPTION_ID
    LEFT JOIN work_orders wo ON wo.SUBSCRIPTION_ID = sh.SUBSCRIPTION_ID
    LEFT JOIN revenue rv     ON rv.SUBSCRIPTION_ID = sh.SUBSCRIPTION_ID
    GROUP BY sh.NODE_ID
),

/* ---------- 5. roll every leaf up to all its ancestors ---------- */
rollup AS (
    SELECT
        an.ANCESTOR_ID                                              AS NODE_ID,
        COUNT(*)                                                    AS SUBTREE_NODES,
        COUNT_IF(n.NODE_TYPE = 'LINE')                              AS SUBTREE_LINES,
        MAX(n.DEPTH)                                                AS SUBTREE_MAX_DEPTH,
        SUM(ZEROIFNULL(l.SERVICES))                                 AS SUBTREE_SERVICES,
        SUM(ZEROIFNULL(l.ACTIVE_SUBS))                              AS SUBTREE_ACTIVE_SUBS,
        SUM(ZEROIFNULL(l.WEAK_ONTS))                                AS SUBTREE_WEAK_ONTS,
        SUM(ZEROIFNULL(l.CRITICAL_ONTS))                            AS SUBTREE_CRITICAL_ONTS,
        SUM(ZEROIFNULL(l.HOT_ONTS))                                 AS SUBTREE_HOT_ONTS,
        SUM(ZEROIFNULL(l.SILENT_ONTS))                              AS SUBTREE_SILENT_ONTS,
        SUM(ZEROIFNULL(l.ONT_REBOOTS))                              AS SUBTREE_ONT_REBOOTS,
        DIV0(SUM(l.SUM_RX_DBM), SUM(l.RX_SAMPLES))                  AS SUBTREE_AVG_RX_DBM,
        SUM(ZEROIFNULL(l.SOLD_MBPS))                                AS SUBTREE_SOLD_MBPS,
        SUM(ZEROIFNULL(l.TICKETS_30D))                              AS SUBTREE_TICKETS_30D,
        SUM(ZEROIFNULL(l.FIBRE_TICKETS_30D))                        AS SUBTREE_FIBRE_TICKETS_30D,
        SUM(ZEROIFNULL(l.OPEN_TICKETS))                             AS SUBTREE_OPEN_TICKETS,
        SUM(ZEROIFNULL(l.WORK_ORDERS_30D))                          AS SUBTREE_WORK_ORDERS_30D,
        SUM(ZEROIFNULL(l.CPE_REPLACED_30D))                         AS SUBTREE_CPE_REPLACED_30D,
        SUM(ZEROIFNULL(l.REVENUE_MTD_PHP))                          AS SUBTREE_REVENUE_MTD_PHP,
        COUNT_IF(l.SERVICES > 0)                                    AS NODES_WITH_SERVICES
    FROM ancestry an
    JOIN nodes n
      ON n.NODE_ID = an.NODE_ID
    LEFT JOIN leaf l
           ON l.NODE_ID = an.NODE_ID
    GROUP BY an.ANCESTOR_ID
),

children AS (
    SELECT
        PARENT_NODE_ID                                              AS NODE_ID,
        COUNT(*)                                                    AS CHILD_COUNT,
        MEDIAN(ST_DISTANCE(LOCATION, PARENT_LOCATION))              AS MEDIAN_CHILD_DISTANCE_M,
        MAX(ST_DISTANCE(LOCATION, PARENT_LOCATION))                 AS MAX_CHILD_DISTANCE_M
    FROM nodes
    WHERE PARENT_NODE_ID IS NOT NULL
    GROUP BY PARENT_NODE_ID
),

/* ---------- 6. outages in the node's region (last 30 days) ---------- */
region_outages AS (
    SELECT
        oi.REGION_ID,
        COUNT(*)                                                    AS REGION_OUTAGES_30D,
        COUNT_IF(oi.ROOT_CAUSE = 'Fiber cut')                       AS REGION_FIBER_CUTS_30D,
        SUM(ZEROIFNULL(oi.OUTAGE_MINUTES))                          AS REGION_OUTAGE_MINUTES_30D
    FROM {{ ref('slv_outage_incident') }} oi
    CROSS JOIN params p
    WHERE oi.EVENT_DATE >= DATEADD('day', -29, p.RUN_DAY)
      AND oi.EVENT_DATE <= p.RUN_DAY
    GROUP BY oi.REGION_ID
),

impacted_subs AS (
    SELECT
        sv.NODE_ID,
        COUNT(DISTINCT im.SUBSCRIPTION_ID)                          AS SUBS_WITH_OUTAGE_30D
    FROM services sv
    CROSS JOIN params p
    JOIN {{ ref('slv_outage_impact') }} im
      ON im.SUBSCRIPTION_ID = sv.SUBSCRIPTION_ID
    JOIN {{ ref('slv_outage_incident') }} oi
      ON oi.INCIDENT_ID = im.INCIDENT_ID
     AND oi.EVENT_DATE >= DATEADD('day', -29, p.RUN_DAY)
     AND oi.EVENT_DATE <= p.RUN_DAY
    GROUP BY sv.NODE_ID
)

SELECT
    p.RUN_DAY                                                       AS SNAPSHOT_DATE,
    n.NODE_ID,
    n.NODE_TYPE,
    n.NODE_NAME,
    n.PARENT_NODE_ID,
    n.ROOT_ID,
    n.ROOT_NAME,
    n.DEPTH,
    ARRAY_TO_STRING(n.PATH, '>')                                    AS PATH_TEXT,
    ARRAY_TO_STRING(n.TYPE_PATH, '>')                               AS TYPE_PATH_TEXT,
    n.REGION_ID,
    COALESCE(g.REGION_NAME, 'UNKNOWN')                              AS REGION_NAME,
    ROUND(ST_Y(n.LOCATION), 6)                                      AS LATITUDE,
    ROUND(ST_X(n.LOCATION), 6)                                      AS LONGITUDE,
    ROUND(ST_DISTANCE(n.LOCATION, n.PARENT_LOCATION), 1)            AS DISTANCE_TO_PARENT_M,
    ZEROIFNULL(ch.CHILD_COUNT)                                      AS CHILD_COUNT,
    ROUND(ch.MEDIAN_CHILD_DISTANCE_M, 1)                            AS MEDIAN_CHILD_DISTANCE_M,
    ROUND(ch.MAX_CHILD_DISTANCE_M, 1)                               AS MAX_CHILD_DISTANCE_M,
    n.CAPACITY,
    r.SUBTREE_NODES,
    r.SUBTREE_LINES,
    r.SUBTREE_MAX_DEPTH - n.DEPTH                                   AS SUBTREE_HEIGHT,
    r.SUBTREE_SERVICES,
    r.SUBTREE_ACTIVE_SUBS,
    ROUND(DIV0(r.SUBTREE_SERVICES, n.CAPACITY), 4)                  AS CAPACITY_UTILISATION,
    r.SUBTREE_WEAK_ONTS,
    r.SUBTREE_CRITICAL_ONTS,
    r.SUBTREE_HOT_ONTS,
    r.SUBTREE_SILENT_ONTS,
    r.SUBTREE_ONT_REBOOTS,
    ROUND(r.SUBTREE_AVG_RX_DBM, 2)                                  AS SUBTREE_AVG_RX_DBM,
    ROUND(DIV0(r.SUBTREE_WEAK_ONTS, r.SUBTREE_SERVICES), 4)         AS WEAK_ONT_RATE,
    r.SUBTREE_SOLD_MBPS,
    r.SUBTREE_TICKETS_30D,
    r.SUBTREE_FIBRE_TICKETS_30D,
    r.SUBTREE_OPEN_TICKETS,
    r.SUBTREE_WORK_ORDERS_30D,
    r.SUBTREE_CPE_REPLACED_30D,
    ROUND(r.SUBTREE_REVENUE_MTD_PHP, 2)                             AS SUBTREE_REVENUE_MTD_PHP,
    ZEROIFNULL(isb.SUBS_WITH_OUTAGE_30D)                            AS DIRECT_SUBS_WITH_OUTAGE_30D,
    ZEROIFNULL(ro.REGION_OUTAGES_30D)                               AS REGION_OUTAGES_30D,
    ZEROIFNULL(ro.REGION_FIBER_CUTS_30D)                            AS REGION_FIBER_CUTS_30D,
    ZEROIFNULL(ro.REGION_OUTAGE_MINUTES_30D)                        AS REGION_OUTAGE_MINUTES_30D,
    ROUND(
          100
        - 40 * LEAST(DIV0(r.SUBTREE_WEAK_ONTS + 2 * r.SUBTREE_CRITICAL_ONTS, r.SUBTREE_SERVICES), 1)
        - 25 * LEAST(DIV0(r.SUBTREE_FIBRE_TICKETS_30D, r.SUBTREE_SERVICES) * 5, 1)
        - 20 * LEAST(DIV0(r.SUBTREE_SERVICES, n.CAPACITY), 1)
        - 15 * LEAST(DIV0(r.SUBTREE_SILENT_ONTS, r.SUBTREE_SERVICES), 1)
    , 1)                                                            AS HEALTH_SCORE,
    CASE
        WHEN r.SUBTREE_SERVICES = 0                                 THEN 'EMPTY'
        WHEN DIV0(r.SUBTREE_CRITICAL_ONTS, r.SUBTREE_SERVICES) > 0.10 THEN 'CRITICAL'
        WHEN DIV0(r.SUBTREE_WEAK_ONTS, r.SUBTREE_SERVICES) > 0.20   THEN 'DEGRADED'
        WHEN DIV0(r.SUBTREE_SERVICES, n.CAPACITY) > 0.90            THEN 'NEAR_FULL'
        ELSE 'HEALTHY'
    END                                                             AS NODE_STATUS,
    RANK() OVER (PARTITION BY n.NODE_TYPE ORDER BY r.SUBTREE_WEAK_ONTS DESC, n.NODE_ID) AS WEAK_RANK_IN_TYPE,
    ROUND(PERCENT_RANK() OVER (PARTITION BY n.ROOT_ID, n.NODE_TYPE ORDER BY r.SUBTREE_REVENUE_MTD_PHP), 4) AS REVENUE_PCT_RANK_IN_OLT,
    CURRENT_TIMESTAMP()                                             AS LOADED_AT
FROM nodes n
CROSS JOIN params p
JOIN rollup r                ON r.NODE_ID = n.NODE_ID
LEFT JOIN children ch        ON ch.NODE_ID = n.NODE_ID
LEFT JOIN impacted_subs isb  ON isb.NODE_ID = n.NODE_ID
LEFT JOIN region_outages ro  ON ro.REGION_ID = n.REGION_ID
LEFT JOIN {{ ref('slv_region') }} g
       ON g.REGION_ID = n.REGION_ID
