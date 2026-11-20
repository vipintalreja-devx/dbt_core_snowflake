{{
  config(
    materialized = 'table',
    transient = true,
    cluster_by = ['created_date', 'customer_region_name'],
    tags = ['gold', 'advanced', 'h10', 'field_service', 'geo'],
    query_tag = 'dbt_telco_adv_h10'
  )
}}
{#-
  adv_nearest_technician  -  Assigned vs nearest technician
  ------------------------------------------------------------------------------------------
  Ported from x_h10_nearest_technician (PIPE_X.SP_X_H10_NEAREST_TECHNICIAN).

  For every work order created in the reporting window, finds the customer's service
  location and compares the assigned technician with the nearest one by straight-line
  distance. Each candidate technician is scored on distance, skill match, region, workload
  that day and contractor status; the best-scored and nearest candidates are kept with the
  top three as a JSON array. Appointment slot, ticket priority and SLA show whether the
  assignment cost time that mattered.

  Grain     : work_order_id (created_date = Manila day the work order was created).
  Window    : work orders created on/after var('history_start_date'); 90-day technician
              track record and repeat visits read the full silver work-order history.
  Materialization: the original was a DYNAMIC TABLE. A dynamic table refreshes as its
              owner role only (no secondary roles); DBT_ROLE has no direct USAGE on
              TELCO_PROD_XL_V2 (needed for ref.geo_zip_population), so this is a transient table
              rebuilt each run. After `GRANT USAGE ON DATABASE TELCO_PROD_XL_V2 (+schema/select)
              TO ROLE DBT_ROLE` it can switch back to materialized='dynamic_table',
              target_lag='1 day', refresh_mode='full'.
  Changed vs original: one RUN_DAY -> every day in the window (workload, track record and
              repeat-visit windows are relative to each work order's own day).
  Snowflake : ST_DISTANCE, H3_POINT_TO_CELL_STRING, ARRAY_CONTAINS, ARRAY_SIZE, MIN_BY,
              MAX_BY, ARRAY_AGG(OBJECT_CONSTRUCT()) WITHIN GROUP, ARRAY_COMPACT, QUALIFY, IFF,
              DECODE, TIMESTAMPDIFF, APPROX_PERCENTILE, BOOLOR_AGG, EQUAL_NULL, dynamic table.
  Sources (11): slv_work_order (read 3x), slv_ticket (2x), slv_service_instance,
              slv_subscription, slv_plan, slv_field_technician, slv_time_slot, slv_region,
              ref.geo_zip_population (bronze).
-#}

WITH params AS (
    SELECT
        '{{ var("history_start_date") }}'::DATE                     AS WINDOW_START,
        40.0                                                        AS AVG_SPEED_KMH,
        60.0                                                        AS MAX_CANDIDATE_KM
),

/* ---------- 1. work orders of the window with the customer's location ---------- */
work_orders AS (
    SELECT
        w.CREATED_DATE,
        w.WORK_ORDER_ID,
        w.TICKET_ID,
        w.TECHNICIAN_ID                                             AS ASSIGNED_TECH_ID,
        w.WORK_TYPE,
        w.CREATED_AT_LOCAL                                          AS CREATED_AT,
        w.DISPATCHED_AT_LOCAL                                       AS DISPATCHED_AT,
        w.ARRIVED_AT_LOCAL                                          AS ARRIVED_AT,
        w.COMPLETED_AT_LOCAL                                        AS COMPLETED_AT,
        w.OUTCOME,
        t.SUBSCRIPTION_ID,
        t.CATEGORY                                                  AS TICKET_CATEGORY,
        t.PRIORITY,
        t.CHANNEL                                                   AS TICKET_CHANNEL,
        t.OPENED_AT_LOCAL                                           AS TICKET_OPENED_AT,
        si.SERVICE_INSTANCE_ID,
        si.SERVICE_TYPE,
        si.SERVICE_LOCATION                                         AS CUSTOMER_LOCATION,
        si.CITY,
        si.BARANGAY,
        si.ZIP_CODE
    FROM {{ ref('slv_work_order') }} w
    CROSS JOIN params p
    JOIN {{ ref('slv_ticket') }} t
      ON t.TICKET_ID = w.TICKET_ID
    JOIN {{ ref('slv_service_instance') }} si
      ON si.SUBSCRIPTION_ID = t.SUBSCRIPTION_ID
    WHERE w.CREATED_DATE >= p.WINDOW_START
      AND si.SERVICE_LOCATION IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY w.WORK_ORDER_ID
        ORDER BY si.IS_ACTIVE DESC, si.VALIDATED_ON DESC NULLS LAST, si.SERVICE_INSTANCE_ID
    ) = 1
),

customer_context AS (
    SELECT
        wo.WORK_ORDER_ID,
        z.REGION_ID                                                 AS CUSTOMER_REGION_ID,
        COALESCE(g.REGION_NAME, 'UNKNOWN')                          AS CUSTOMER_REGION_NAME,
        z.POPULATION,
        ROUND(DIV0(z.POPULATION, z.AREA_KM2), 1)                    AS POP_DENSITY_PER_KM2,
        s.PLAN_ID,
        pc.CONTRACT_TYPE,
        pc.SPEED_MBPS                                               AS PLAN_SPEED_MBPS,
        H3_POINT_TO_CELL_STRING(wo.CUSTOMER_LOCATION, 7)            AS CUSTOMER_H3_R7
    FROM work_orders wo
    LEFT JOIN {{ source('ref', 'geo_zip_population') }} z
           ON z.ZIP_CODE = wo.ZIP_CODE
    LEFT JOIN {{ ref('slv_region') }} g
           ON g.REGION_ID = z.REGION_ID
    LEFT JOIN {{ ref('slv_subscription') }} s
           ON s.SUBSCRIPTION_ID = wo.SUBSCRIPTION_ID
    LEFT JOIN {{ ref('slv_plan') }} pc
           ON pc.PLAN_ID = s.PLAN_ID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY wo.WORK_ORDER_ID ORDER BY z.POPULATION DESC NULLS LAST) = 1
),

appointment AS (
    SELECT
        w.WORK_ORDER_ID,
        w.SLOT_DATE,
        w.SLOT_START_LOCAL                                          AS SLOT_START_AT,
        w.SLOT_END_LOCAL                                            AS SLOT_END_AT,
        w.IS_SLOT_CONFIRMED                                         AS CONFIRMED,
        ts.SLOT_LABEL,
        COALESCE(ts.IS_PEAK, FALSE)                                 AS IS_PEAK_SLOT
    FROM {{ ref('slv_work_order') }} w
    LEFT JOIN {{ ref('slv_time_slot') }} ts
           ON ts.SLOT_ID = w.SLOT_ID
    WHERE w.WORK_ORDER_ID IN (SELECT WORK_ORDER_ID FROM work_orders)
),

/* ---------- 2. technicians and their workload per day ---------- */
technicians AS (
    SELECT
        f.TECHNICIAN_ID,
        f.REGION_ID                                                 AS TECH_REGION_ID,
        f.SKILLS,
        f.SKILL_COUNT,
        f.HOME_BASE,
        f.IS_CONTRACTOR,
        f.HIRED_ON,
        H3_POINT_TO_CELL_STRING(f.HOME_BASE, 7)                     AS TECH_H3_R7
    FROM {{ ref('slv_field_technician') }} f
    WHERE f.HOME_BASE IS NOT NULL
),

workload AS (
    SELECT
        w.CREATED_DATE,
        w.TECHNICIAN_ID,
        COUNT(*)                                                    AS JOBS_THAT_DAY,
        COUNT_IF(w.COMPLETED_AT_LOCAL IS NULL)                      AS OPEN_JOBS_THAT_DAY
    FROM {{ ref('slv_work_order') }} w
    CROSS JOIN params p
    WHERE w.CREATED_DATE >= p.WINDOW_START
    GROUP BY w.CREATED_DATE, w.TECHNICIAN_ID
),

/* ---------- 2b. 90-day track record (relative to each work-order day) ---------- */
order_days AS (
    SELECT DISTINCT CREATED_DATE FROM work_orders
),

tech_history AS (
    SELECT
        d.CREATED_DATE,
        w.TECHNICIAN_ID,
        COUNT(*)                                                    AS JOBS_90D,
        COUNT_IF(w.OUTCOME IN ('FIXED', 'REPLACED_CPE'))            AS RESOLVED_JOBS_90D,
        COUNT_IF(w.OUTCOME = 'RESCHEDULED')                         AS RESCHEDULED_JOBS_90D,
        COUNT_IF(w.OUTCOME = 'NO_FAULT_FOUND')                      AS NO_FAULT_JOBS_90D,
        AVG(TIMESTAMPDIFF('minute', w.ARRIVED_AT_LOCAL, w.COMPLETED_AT_LOCAL))  AS AVG_ON_SITE_MIN_90D,
        AVG(TIMESTAMPDIFF('minute', w.DISPATCHED_AT_LOCAL, w.ARRIVED_AT_LOCAL)) AS AVG_TRAVEL_MIN_90D,
        APPROX_PERCENTILE(TIMESTAMPDIFF('minute', w.CREATED_AT_LOCAL, w.COMPLETED_AT_LOCAL), 0.9) AS P90_TOTAL_MIN_90D,
        COUNT(DISTINCT w.CREATED_DATE)                              AS WORKING_DAYS_90D
    FROM order_days d
    JOIN {{ ref('slv_work_order') }} w
      ON w.CREATED_DATE >= DATEADD('day', -90, d.CREATED_DATE)
     AND w.CREATED_DATE <  d.CREATED_DATE
    GROUP BY d.CREATED_DATE, w.TECHNICIAN_ID
),

repeat_visits AS (
    SELECT
        wo.WORK_ORDER_ID,
        COUNT(prev.WORK_ORDER_ID)                                   AS PRIOR_VISITS_90D,
        MAX(prev.CREATED_AT_LOCAL)                                  AS LAST_PRIOR_VISIT_AT,
        MAX_BY(prev.TECHNICIAN_ID, prev.CREATED_AT_LOCAL)           AS LAST_PRIOR_TECH_ID,
        MAX_BY(prev.OUTCOME, prev.CREATED_AT_LOCAL)                 AS LAST_PRIOR_OUTCOME,
        BOOLOR_AGG(prev.TECHNICIAN_ID = wo.ASSIGNED_TECH_ID)        AS SAME_TECH_BEFORE
    FROM work_orders wo
    LEFT JOIN {{ ref('slv_ticket') }} pt
           ON pt.SUBSCRIPTION_ID = wo.SUBSCRIPTION_ID
    LEFT JOIN {{ ref('slv_work_order') }} prev
           ON prev.TICKET_ID = pt.TICKET_ID
          AND prev.CREATED_DATE >= DATEADD('day', -90, wo.CREATED_DATE)
          AND prev.CREATED_AT_LOCAL < wo.CREATED_AT
    GROUP BY wo.WORK_ORDER_ID
),

region_capacity AS (
    SELECT
        d.CREATED_DATE,
        tc.TECH_REGION_ID,
        COUNT(*)                                                    AS TECHS_IN_REGION,
        COUNT_IF(tc.IS_CONTRACTOR)                                  AS CONTRACTORS_IN_REGION,
        SUM(ZEROIFNULL(wl.JOBS_THAT_DAY))                           AS REGION_JOBS_THAT_DAY
    FROM order_days d
    JOIN technicians tc
      ON tc.HIRED_ON IS NULL OR tc.HIRED_ON <= d.CREATED_DATE
    LEFT JOIN workload wl
           ON wl.TECHNICIAN_ID = tc.TECHNICIAN_ID
          AND wl.CREATED_DATE = d.CREATED_DATE
    GROUP BY d.CREATED_DATE, tc.TECH_REGION_ID
),

/* ---------- 3. every technician hired by that day as a candidate for every work order ---------- */
candidates AS (
    SELECT
        wo.CREATED_DATE,
        wo.WORK_ORDER_ID,
        wo.ASSIGNED_TECH_ID,
        tc.TECHNICIAN_ID,
        tc.TECH_REGION_ID,
        tc.IS_CONTRACTOR,
        tc.SKILL_COUNT,
        DATEDIFF('month', tc.HIRED_ON, wo.CREATED_DATE)             AS MONTHS_OF_SERVICE,
        ROUND(ST_DISTANCE(wo.CUSTOMER_LOCATION, tc.HOME_BASE) / 1000, 3) AS KM,
        IFF(ARRAY_CONTAINS(wo.WORK_TYPE::VARIANT, tc.SKILLS)
            OR ARRAY_CONTAINS(wo.SERVICE_TYPE::VARIANT, tc.SKILLS), 1, 0) AS SKILL_MATCH,
        IFF(tc.TECH_REGION_ID = cc.CUSTOMER_REGION_ID, 1, 0)        AS SAME_REGION,
        IFF(tc.TECH_H3_R7 = cc.CUSTOMER_H3_R7, 1, 0)                AS SAME_H3_CELL,
        ZEROIFNULL(wl.JOBS_THAT_DAY)                                AS JOBS_THAT_DAY,
        ZEROIFNULL(wl.OPEN_JOBS_THAT_DAY)                           AS OPEN_JOBS_THAT_DAY
    FROM work_orders wo
    JOIN customer_context cc
      ON cc.WORK_ORDER_ID = wo.WORK_ORDER_ID
    JOIN technicians tc
      ON tc.HIRED_ON IS NULL OR tc.HIRED_ON <= wo.CREATED_DATE
    LEFT JOIN workload wl
           ON wl.TECHNICIAN_ID = tc.TECHNICIAN_ID
          AND wl.CREATED_DATE = wo.CREATED_DATE
),

scored AS (
    SELECT
        c.*,
        ROUND(
              0.50 * LEAST(c.KM / 60.0, 1)
            + 0.20 * (1 - c.SKILL_MATCH)
            + 0.15 * (1 - c.SAME_REGION)
            + 0.10 * LEAST(c.OPEN_JOBS_THAT_DAY / 4, 1)
            + 0.05 * IFF(c.IS_CONTRACTOR, 1, 0)
        , 5)                                                        AS COST_SCORE,
        RANK() OVER (PARTITION BY c.WORK_ORDER_ID ORDER BY c.KM, c.TECHNICIAN_ID) AS DISTANCE_RANK
    FROM candidates c
),

ranked AS (
    SELECT
        s.*,
        RANK() OVER (PARTITION BY s.WORK_ORDER_ID ORDER BY s.COST_SCORE, s.KM, s.TECHNICIAN_ID) AS SCORE_RANK
    FROM scored s
),

per_order AS (
    SELECT
        r.WORK_ORDER_ID,
        MAX(IFF(r.TECHNICIAN_ID = r.ASSIGNED_TECH_ID, r.KM, NULL))  AS ASSIGNED_KM,
        MAX(IFF(r.TECHNICIAN_ID = r.ASSIGNED_TECH_ID, r.DISTANCE_RANK, NULL)) AS ASSIGNED_DISTANCE_RANK,
        MAX(IFF(r.TECHNICIAN_ID = r.ASSIGNED_TECH_ID, r.SCORE_RANK, NULL))    AS ASSIGNED_SCORE_RANK,
        MAX(IFF(r.TECHNICIAN_ID = r.ASSIGNED_TECH_ID, r.SKILL_MATCH, NULL))   AS ASSIGNED_SKILL_MATCH,
        MAX(IFF(r.TECHNICIAN_ID = r.ASSIGNED_TECH_ID, r.SAME_REGION, NULL))   AS ASSIGNED_SAME_REGION,
        MAX(IFF(r.TECHNICIAN_ID = r.ASSIGNED_TECH_ID, r.JOBS_THAT_DAY, NULL)) AS ASSIGNED_JOBS_THAT_DAY,
        MIN_BY(r.TECHNICIAN_ID, r.KM)                               AS NEAREST_TECH_ID,
        MIN(r.KM)                                                   AS NEAREST_KM,
        MIN_BY(r.TECHNICIAN_ID, r.COST_SCORE)                       AS BEST_TECH_ID,
        MIN_BY(r.KM, r.COST_SCORE)                                  AS BEST_TECH_KM,
        MIN(r.COST_SCORE)                                           AS BEST_COST_SCORE,
        COUNT(*)                                                    AS CANDIDATES,
        COUNT_IF(r.KM <= 60.0)                                      AS CANDIDATES_IN_RANGE,
        COUNT_IF(r.SKILL_MATCH = 1)                                 AS SKILLED_CANDIDATES,
        COUNT_IF(r.SAME_H3_CELL = 1)                                AS CANDIDATES_IN_SAME_H3,
        ARRAY_AGG(
            IFF(r.DISTANCE_RANK <= 3,
                OBJECT_CONSTRUCT('technician_id', r.TECHNICIAN_ID, 'km', r.KM,
                                 'skill_match', r.SKILL_MATCH, 'open_jobs', r.OPEN_JOBS_THAT_DAY),
                NULL)
        ) WITHIN GROUP (ORDER BY r.DISTANCE_RANK)                  AS TOP3_NEAREST
    FROM ranked r
    GROUP BY r.WORK_ORDER_ID
)

SELECT
    wo.CREATED_DATE,
    wo.WORK_ORDER_ID,
    wo.TICKET_ID,
    wo.SUBSCRIPTION_ID,
    wo.WORK_TYPE,
    wo.SERVICE_TYPE,
    wo.TICKET_CATEGORY,
    wo.PRIORITY,
    wo.TICKET_CHANNEL,
    wo.OUTCOME,
    wo.CITY,
    wo.BARANGAY,
    cc.CUSTOMER_REGION_NAME,
    cc.CUSTOMER_H3_R7,
    cc.POP_DENSITY_PER_KM2,
    IFF(cc.POP_DENSITY_PER_KM2 >= 5000, 'URBAN', IFF(cc.POP_DENSITY_PER_KM2 >= 500, 'SUBURBAN', 'RURAL')) AS AREA_TYPE,
    cc.PLAN_ID,
    cc.CONTRACT_TYPE,
    cc.PLAN_SPEED_MBPS,
    wo.ASSIGNED_TECH_ID,
    po.ASSIGNED_KM,
    po.ASSIGNED_DISTANCE_RANK,
    po.ASSIGNED_SCORE_RANK,
    po.ASSIGNED_SKILL_MATCH,
    po.ASSIGNED_SAME_REGION,
    po.ASSIGNED_JOBS_THAT_DAY,
    po.NEAREST_TECH_ID,
    po.NEAREST_KM,
    po.BEST_TECH_ID,
    po.BEST_TECH_KM,
    po.BEST_COST_SCORE,
    ROUND(po.ASSIGNED_KM - po.NEAREST_KM, 3)                        AS EXTRA_KM,
    ROUND((po.ASSIGNED_KM - po.NEAREST_KM) / p.AVG_SPEED_KMH * 60, 1) AS EXTRA_TRAVEL_MIN,
    EQUAL_NULL(po.NEAREST_TECH_ID, wo.ASSIGNED_TECH_ID)             AS IS_NEAREST,
    EQUAL_NULL(po.BEST_TECH_ID, wo.ASSIGNED_TECH_ID)                AS IS_BEST_SCORED,
    po.CANDIDATES,
    po.CANDIDATES_IN_RANGE,
    po.SKILLED_CANDIDATES,
    po.CANDIDATES_IN_SAME_H3,
    ARRAY_COMPACT(po.TOP3_NEAREST)                                  AS TOP3_NEAREST,
    th.JOBS_90D                                                     AS ASSIGNED_JOBS_90D,
    ROUND(DIV0(th.RESOLVED_JOBS_90D, th.JOBS_90D), 4)               AS ASSIGNED_FIX_RATE_90D,
    ROUND(DIV0(th.RESCHEDULED_JOBS_90D, th.JOBS_90D), 4)            AS ASSIGNED_RESCHEDULE_RATE_90D,
    ROUND(th.AVG_ON_SITE_MIN_90D, 1)                                AS ASSIGNED_AVG_ON_SITE_MIN_90D,
    ROUND(th.AVG_TRAVEL_MIN_90D, 1)                                 AS ASSIGNED_AVG_TRAVEL_MIN_90D,
    th.P90_TOTAL_MIN_90D                                            AS ASSIGNED_P90_TOTAL_MIN_90D,
    th.WORKING_DAYS_90D                                             AS ASSIGNED_WORKING_DAYS_90D,
    nth.JOBS_90D                                                    AS NEAREST_JOBS_90D,
    ROUND(DIV0(nth.RESOLVED_JOBS_90D, nth.JOBS_90D), 4)             AS NEAREST_FIX_RATE_90D,
    ZEROIFNULL(rv.PRIOR_VISITS_90D)                                 AS PRIOR_VISITS_90D,
    rv.LAST_PRIOR_VISIT_AT,
    rv.LAST_PRIOR_TECH_ID,
    rv.LAST_PRIOR_OUTCOME,
    COALESCE(rv.SAME_TECH_BEFORE, FALSE)                            AS SAME_TECH_BEFORE,
    IFF(rv.PRIOR_VISITS_90D > 0, TRUE, FALSE)                       AS IS_REPEAT_VISIT,
    rc.TECHS_IN_REGION                                              AS CUSTOMER_REGION_TECHS,
    rc.CONTRACTORS_IN_REGION                                        AS CUSTOMER_REGION_CONTRACTORS,
    rc.REGION_JOBS_THAT_DAY                                         AS CUSTOMER_REGION_JOBS_THAT_DAY,
    ROUND(DIV0(rc.REGION_JOBS_THAT_DAY, rc.TECHS_IN_REGION), 2)     AS REGION_JOBS_PER_TECH,
    ap.SLOT_LABEL,
    ap.IS_PEAK_SLOT,
    ap.CONFIRMED                                                    AS SLOT_CONFIRMED,
    TIMESTAMPDIFF('minute', wo.CREATED_AT, wo.DISPATCHED_AT)        AS MIN_TO_DISPATCH,
    TIMESTAMPDIFF('minute', wo.DISPATCHED_AT, wo.ARRIVED_AT)        AS MIN_TO_ARRIVE,
    TIMESTAMPDIFF('minute', wo.ARRIVED_AT, wo.COMPLETED_AT)         AS MIN_ON_SITE,
    IFF(wo.ARRIVED_AT BETWEEN ap.SLOT_START_AT AND ap.SLOT_END_AT, TRUE, FALSE) AS ARRIVED_IN_SLOT,
    DECODE(wo.PRIORITY, 'P1', 4, 'P2', 8, 'P3', 24, 'P4', 72, 48)  AS SLA_HOURS,
    IFF(TIMESTAMPDIFF('hour', wo.CREATED_AT, COALESCE(wo.COMPLETED_AT, DATEADD('day', 1, wo.CREATED_DATE)::TIMESTAMP_NTZ))
        > DECODE(wo.PRIORITY, 'P1', 4, 'P2', 8, 'P3', 24, 'P4', 72, 48), TRUE, FALSE) AS SLA_BREACHED,
    CASE
        WHEN po.ASSIGNED_KM IS NULL                                 THEN 'ASSIGNED_TECH_UNKNOWN'
        WHEN EQUAL_NULL(po.NEAREST_TECH_ID, wo.ASSIGNED_TECH_ID)    THEN 'OPTIMAL'
        WHEN po.ASSIGNED_KM - po.NEAREST_KM <= 5                    THEN 'ACCEPTABLE'
        WHEN po.ASSIGNED_SKILL_MATCH = 1 AND po.SKILLED_CANDIDATES <= 3 THEN 'SKILL_CONSTRAINED'
        WHEN COALESCE(rv.SAME_TECH_BEFORE, FALSE)                   THEN 'CONTINUITY_CHOICE'
        ELSE 'SUBOPTIMAL'
    END                                                             AS ASSIGNMENT_QUALITY,
    CURRENT_TIMESTAMP()                                             AS LOADED_AT
FROM work_orders wo
CROSS JOIN params p
JOIN customer_context cc   ON cc.WORK_ORDER_ID = wo.WORK_ORDER_ID
JOIN per_order po          ON po.WORK_ORDER_ID = wo.WORK_ORDER_ID
LEFT JOIN appointment ap   ON ap.WORK_ORDER_ID = wo.WORK_ORDER_ID
LEFT JOIN tech_history th  ON th.TECHNICIAN_ID = wo.ASSIGNED_TECH_ID AND th.CREATED_DATE = wo.CREATED_DATE
LEFT JOIN tech_history nth ON nth.TECHNICIAN_ID = po.NEAREST_TECH_ID AND nth.CREATED_DATE = wo.CREATED_DATE
LEFT JOIN repeat_visits rv ON rv.WORK_ORDER_ID = wo.WORK_ORDER_ID
LEFT JOIN region_capacity rc ON rc.TECH_REGION_ID = cc.CUSTOMER_REGION_ID AND rc.CREATED_DATE = wo.CREATED_DATE
