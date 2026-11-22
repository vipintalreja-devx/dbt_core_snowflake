{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'kpi_date',
    on_schema_change = 'append_new_columns',
    cluster_by = ['kpi_date', 'region_id'],
    tags = ['gold', 'advanced', 'h14', 'network', 'anomaly'],
    query_tag = 'dbt_telco_h14'
  )
}}
{#-
  adv_cell_anomaly  -  Daily cell KPI anomaly detection
  ------------------------------------------------------------------------------------------
  Ported from x_h14_py_cell_anomaly (PIPE_X.SP_X_H14_PY_CELL_ANOMALY, originally Python).
  Grain : one row per cell per local kpi_date.
  For every day it compares the cell's daily KPIs with its own baseline over the previous
  29 days (classic z-score and robust MAD z-score), checks configured thresholds (unit-
  normalised), and attaches alarms, equipment state, home-subscriber tickets and regional
  outages to classify the most likely cause.

  Changes vs. original:
    * every day in the window instead of one run_date; baseline via a 29-day range join
      (MEDIAN / APPROX_PERCENTILE cannot use sliding window frames);
    * reads the silver hourly roll-up (slv_cell_kpi_hourly: Manila hours, NaN cleaned,
      drop rate in %) instead of 315M raw 15-min rows; the original compared the drop-rate
      FRACTION against %-thresholds, which never breached;
    * incremental table instead of a view.

  Snowflake-specific: STDDEV_SAMP, MEDIAN, APPROX_PERCENTILE, MIN_BY/MAX_BY, MODE,
  LISTAGG DISTINCT ... WITHIN GROUP, ARRAY_AGG DISTINCT, ARRAY_SIZE, TRY_TO_NUMBER,
  VARIANT path access, CONVERT_TIMEZONE, DIV0, ZEROIFNULL, IFF, QUALIFY, RANK.
  Refs/sources (12): slv_cell_kpi_hourly, source cell_kpi_threshold, source ext.unit_conversion,
  source network.alarm_event, slv_network_element, source ne_port_inventory, slv_ticket,
  slv_subscription, slv_outage_incident, slv_cell_site, dim_date (spine), fct_cell_kpi_daily (max date).
-#}

WITH params AS (
    SELECT
        d.run_date,
        d.next_date,
        DATEADD('day', -29, d.run_date)                             AS window_start,
        CONVERT_TIMEZONE('{{ var("local_tz") }}', 'UTC', d.run_date::TIMESTAMP_NTZ)  AS day_start_utc,
        CONVERT_TIMEZONE('{{ var("local_tz") }}', 'UTC', d.next_date::TIMESTAMP_NTZ) AS day_end_utc,
        3.0                                                         AS z_limit,
        3.5                                                         AS robust_limit
    FROM ({{ f4_days_cte('kpi_date', "(select max(event_date) from " ~ ref('slv_cell_kpi_hourly') ~ ")") }}) d
),

/* ---------- 1. hourly KPIs (silver), daily roll-up incl. 29-day look-back ---------- */
hourly AS (
    SELECT
        h.cell_id,
        h.event_date                                                AS kpi_date,
        h.hour_local                                                AS kpi_hour,
        h.throughput_mbps_avg                                       AS throughput_mbps,
        h.drop_rate_pct_avg                                         AS drop_rate_pct,
        h.latency_ms_avg                                            AS latency_ms,
        h.prb_pct_avg                                               AS prb_pct,
        h.prb_pct_max,
        h.active_users_max                                          AS active_users,
        h.pm_intervals
    FROM {{ ref('slv_cell_kpi_hourly') }} h
    WHERE h.event_date >= (SELECT MIN(window_start) FROM params)
      AND h.event_date <  (SELECT MAX(next_date) FROM params)
),

daily AS (
    SELECT
        cell_id,
        kpi_date,
        AVG(throughput_mbps)::FLOAT                                 AS thr,
        AVG(drop_rate_pct)::FLOAT                                   AS drp,
        AVG(latency_ms)::FLOAT                                      AS lat,
        AVG(prb_pct)::FLOAT                                         AS prb,
        AVG(active_users)::FLOAT                                    AS users,
        MAX(prb_pct_max)                                            AS prb_max,
        SUM(pm_intervals)                                           AS intervals,
        COUNT_IF(throughput_mbps IS NULL OR latency_ms IS NULL)     AS null_hours
    FROM hourly
    GROUP BY cell_id, kpi_date
),

/* ---------- 2. baseline over the previous 29 days (per run day) ---------- */
baseline AS (
    SELECT
        p.run_date,
        d.cell_id,
        COUNT(*)                                                    AS baseline_days,
        AVG(d.thr)                                                  AS thr_mean,
        STDDEV_SAMP(d.thr)                                          AS thr_std,
        MEDIAN(d.thr)                                               AS thr_med,
        AVG(d.drp)                                                  AS drp_mean,
        STDDEV_SAMP(d.drp)                                          AS drp_std,
        MEDIAN(d.drp)                                               AS drp_med,
        AVG(d.lat)                                                  AS lat_mean,
        STDDEV_SAMP(d.lat)                                          AS lat_std,
        MEDIAN(d.lat)                                               AS lat_med,
        AVG(d.prb)                                                  AS prb_mean,
        STDDEV_SAMP(d.prb)                                          AS prb_std,
        AVG(d.users)                                                AS users_mean,
        STDDEV_SAMP(d.users)                                        AS users_std,
        APPROX_PERCENTILE(d.thr, 0.05)                              AS thr_p05,
        APPROX_PERCENTILE(d.lat, 0.95)                              AS lat_p95
    FROM params p
    JOIN daily d
      ON d.kpi_date >= p.window_start
     AND d.kpi_date <  p.run_date
    GROUP BY p.run_date, d.cell_id
),

/* median absolute deviation, for a robust z-score */
mad AS (
    SELECT
        p.run_date,
        d.cell_id,
        MEDIAN(ABS(d.thr - b.thr_med))                              AS thr_mad,
        MEDIAN(ABS(d.drp - b.drp_med))                              AS drp_mad,
        MEDIAN(ABS(d.lat - b.lat_med))                              AS lat_mad
    FROM params p
    JOIN daily d
      ON d.kpi_date >= p.window_start
     AND d.kpi_date <  p.run_date
    JOIN baseline b
      ON b.cell_id = d.cell_id
     AND b.run_date = p.run_date
    GROUP BY p.run_date, d.cell_id
),

/* ---------- 3. the run days ---------- */
today AS (
    SELECT p.run_date, d.*
    FROM params p
    JOIN daily d
      ON d.kpi_date = p.run_date
),

worst_hour AS (
    SELECT
        h.cell_id,
        h.kpi_date,
        MIN_BY(h.kpi_hour, h.throughput_mbps)                       AS worst_throughput_hour,
        MIN(h.throughput_mbps)                                      AS worst_hour_throughput,
        MAX_BY(h.kpi_hour, h.latency_ms)                            AS worst_latency_hour,
        MAX(h.latency_ms)                                           AS worst_hour_latency,
        MAX_BY(h.kpi_hour, h.active_users)                          AS busy_hour
    FROM hourly h
    WHERE h.kpi_date IN (SELECT run_date FROM params)
    GROUP BY h.cell_id, h.kpi_date
),

/* ---------- 4. configured thresholds (units normalised; drop rate on % scale) ---------- */
thresholds AS (
    SELECT
        th.cell_id,
        UPPER(th.kpi_name)                                          AS kpi_name,
        th.comparison,
        th.warning_threshold  * COALESCE(uc.factor, 1) + COALESCE(uc.offset_value, 0) AS warning_threshold,
        th.critical_threshold * COALESCE(uc.factor, 1) + COALESCE(uc.offset_value, 0) AS critical_threshold,
        TRIM(th.unit)                                               AS unit
    FROM {{ source('network', 'cell_kpi_threshold') }} th
    LEFT JOIN {{ source('ref', 'unit_conversion') }} uc
           ON TRIM(uc.from_unit) = TRIM(th.unit)
          AND uc.to_unit IN ('Mbit/s', 'ms', '%')
    WHERE COALESCE(th.is_enabled, TRUE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY th.cell_id, th.kpi_name ORDER BY th.threshold_id DESC) = 1
),

threshold_breach AS (
    SELECT
        t.run_date,
        t.cell_id,
        COUNT_IF(
            (th.kpi_name = 'AVG_THROUGHPUT'  AND th.comparison = '<' AND t.thr < th.critical_threshold)
         OR (th.kpi_name = 'LATENCY'         AND th.comparison = '>' AND t.lat > th.critical_threshold)
         OR (th.kpi_name = 'DROP_CALL_RATE'  AND th.comparison = '>' AND t.drp > th.critical_threshold)
         OR (th.kpi_name = 'PRB_UTILIZATION' AND th.comparison = '>' AND t.prb > th.critical_threshold)
        )                                                           AS critical_breaches,
        COUNT_IF(
            (th.kpi_name = 'AVG_THROUGHPUT'  AND th.comparison = '<' AND t.thr < th.warning_threshold)
         OR (th.kpi_name = 'LATENCY'         AND th.comparison = '>' AND t.lat > th.warning_threshold)
         OR (th.kpi_name = 'DROP_CALL_RATE'  AND th.comparison = '>' AND t.drp > th.warning_threshold)
         OR (th.kpi_name = 'PRB_UTILIZATION' AND th.comparison = '>' AND t.prb > th.warning_threshold)
        )                                                           AS warning_breaches,
        LISTAGG(DISTINCT th.unit, ',') WITHIN GROUP (ORDER BY th.unit) AS threshold_units
    FROM today t
    JOIN thresholds th
      ON th.cell_id = t.cell_id
    GROUP BY t.run_date, t.cell_id
),

/* ---------- 5. what else happened on the cell ---------- */
alarms AS (
    SELECT
        CONVERT_TIMEZONE('{{ var("local_tz") }}', a.raised_at)::DATE AS run_date,
        a.cell_id,
        COUNT(*)                                                    AS alarms,
        COUNT_IF(UPPER(a.severity) IN ('CRITICAL', 'MAJOR'))        AS serious_alarms,
        ARRAY_AGG(DISTINCT a.alarm_code)                            AS alarm_codes,
        MAX_BY(a.alarm_code, a.raised_at)                           AS last_alarm_code,
        SUM(TRY_TO_NUMBER(a.details:repeat_count::STRING))          AS alarm_repeats
    FROM {{ source('network', 'alarm_event') }} a
    WHERE a.raised_at >= (SELECT MIN(day_start_utc) FROM params)::TIMESTAMP_NTZ::TIMESTAMP_TZ
      AND a.raised_at <  DATEADD('day', 1, (SELECT MAX(day_end_utc) FROM params))::TIMESTAMP_NTZ::TIMESTAMP_TZ
    GROUP BY 1, 2
),

elements AS (
    SELECT
        ne.cell_id,
        COUNT(DISTINCT ne.element_id)                               AS elements,
        MODE(ne.vendor)                                             AS vendor,
        LISTAGG(DISTINCT ne.firmware_version, ',') WITHIN GROUP (ORDER BY ne.firmware_version) AS firmware_versions,
        COUNT_IF(pt.oper_status = 'DOWN')                           AS ports_down,
        COUNT(pt.port_id)                                           AS ports
    FROM {{ ref('slv_network_element') }} ne
    LEFT JOIN {{ source('network', 'ne_port_inventory') }} pt
           ON pt.element_id = ne.element_id
    GROUP BY ne.cell_id
),

home_tickets AS (
    SELECT
        t.opened_date                                               AS run_date,
        s.home_cell_id                                              AS cell_id,
        COUNT(*)                                                    AS tickets_today,
        COUNT_IF(t.category IN ('NO_INTERNET', 'SLOW_SPEED'))       AS network_tickets_today
    FROM {{ ref('slv_ticket') }} t
    JOIN {{ ref('slv_subscription') }} s
      ON s.subscription_id = t.subscription_id
    WHERE t.opened_date IN (SELECT run_date FROM params)
    GROUP BY 1, 2
),

region_outages AS (
    SELECT
        p.run_date,
        o.region_id,
        COUNT(*)                                                    AS region_outages_today,
        MAX_BY(o.root_cause, o.detected_at_utc)                     AS last_outage_cause
    FROM params p
    JOIN {{ ref('slv_outage_incident') }} o
      ON o.detected_at_utc < p.day_end_utc
     AND COALESCE(o.restored_at_utc, p.day_end_utc) >= p.day_start_utc
    GROUP BY 1, 2
),

/* ---------- 6. scores ---------- */
scored AS (
    SELECT
        t.run_date,
        t.cell_id,
        t.thr,
        t.drp,
        t.lat,
        t.prb,
        t.users,
        t.prb_max,
        t.intervals,
        t.null_hours,
        b.baseline_days,
        b.thr_mean,
        b.drp_mean,
        b.lat_mean,
        b.prb_mean,
        b.users_mean,
        b.thr_p05,
        b.lat_p95,
        (t.thr - b.thr_mean) / NULLIF(b.thr_std, 0)                 AS throughput_z,
        (t.drp - b.drp_mean) / NULLIF(b.drp_std, 0)                 AS drop_rate_z,
        (t.lat - b.lat_mean) / NULLIF(b.lat_std, 0)                 AS latency_z,
        (t.prb - b.prb_mean) / NULLIF(b.prb_std, 0)                 AS prb_z,
        (t.users - b.users_mean) / NULLIF(b.users_std, 0)           AS users_z,
        0.6745 * (t.thr - b.thr_med) / NULLIF(m.thr_mad, 0)         AS throughput_robust,
        0.6745 * (t.drp - b.drp_med) / NULLIF(m.drp_mad, 0)         AS drop_rate_robust,
        0.6745 * (t.lat - b.lat_med) / NULLIF(m.lat_mad, 0)         AS latency_robust
    FROM today t
    JOIN baseline b
      ON b.cell_id = t.cell_id AND b.run_date = t.run_date
    LEFT JOIN mad m
           ON m.cell_id = t.cell_id AND m.run_date = t.run_date
)

SELECT
    sc.run_date                                                     AS kpi_date,
    sc.cell_id,
    cs.site_name,
    cs.radio_type,
    cs.region_id,
    COALESCE(cs.region_name, 'UNKNOWN')                             AS region_name,
    ROUND(sc.thr, 4)                                                AS throughput_mbps,
    ROUND(sc.drp, 6)                                                AS drop_rate_pct,
    ROUND(sc.lat, 3)                                                AS latency_ms,
    ROUND(sc.prb, 3)                                                AS prb_pct,
    ROUND(sc.prb_max, 3)                                            AS prb_max_pct,
    ROUND(sc.users, 2)                                              AS active_users,
    sc.intervals,
    sc.null_hours,
    sc.baseline_days,
    ROUND(sc.thr_mean, 4)                                           AS throughput_baseline,
    ROUND(sc.lat_mean, 3)                                           AS latency_baseline,
    ROUND(sc.throughput_z, 4)                                       AS throughput_z,
    ROUND(sc.drop_rate_z, 4)                                        AS drop_rate_z,
    ROUND(sc.latency_z, 4)                                          AS latency_z,
    ROUND(sc.prb_z, 4)                                              AS prb_z,
    ROUND(sc.users_z, 4)                                            AS users_z,
    ROUND(sc.throughput_robust, 4)                                  AS throughput_robust_z,
    ROUND(sc.drop_rate_robust, 4)                                   AS drop_rate_robust_z,
    ROUND(sc.latency_robust, 4)                                     AS latency_robust_z,
    COALESCE(sc.throughput_z < -p.z_limit, FALSE)
        OR COALESCE(sc.drop_rate_z > p.z_limit, FALSE)              AS is_anomaly,
    COALESCE(sc.throughput_robust < -p.robust_limit, FALSE)
        OR COALESCE(sc.drop_rate_robust > p.robust_limit, FALSE)
        OR COALESCE(sc.latency_robust > p.robust_limit, FALSE)      AS is_robust_anomaly,
    IFF(sc.thr < sc.thr_p05, TRUE, FALSE)                           AS below_p05_throughput,
    IFF(sc.lat > sc.lat_p95, TRUE, FALSE)                           AS above_p95_latency,
    wh.worst_throughput_hour,
    ROUND(wh.worst_hour_throughput, 4)                              AS worst_hour_throughput,
    wh.worst_latency_hour,
    ROUND(wh.worst_hour_latency, 3)                                 AS worst_hour_latency,
    wh.busy_hour,
    ZEROIFNULL(tb.critical_breaches)                                AS critical_threshold_breaches,
    ZEROIFNULL(tb.warning_breaches)                                 AS warning_threshold_breaches,
    tb.threshold_units,
    ZEROIFNULL(al.alarms)                                           AS alarms_today,
    ZEROIFNULL(al.serious_alarms)                                   AS serious_alarms_today,
    ARRAY_SIZE(al.alarm_codes)                                      AS distinct_alarm_codes,
    al.last_alarm_code,
    ZEROIFNULL(al.alarm_repeats)                                    AS alarm_repeats,
    ZEROIFNULL(el.elements)                                         AS elements,
    el.vendor,
    el.firmware_versions,
    ZEROIFNULL(el.ports_down)                                       AS ports_down,
    ROUND(DIV0(el.ports_down, el.ports), 4)                         AS port_down_rate,
    ZEROIFNULL(ht.tickets_today)                                    AS home_sub_tickets_today,
    ZEROIFNULL(ht.network_tickets_today)                            AS home_sub_network_tickets_today,
    ZEROIFNULL(ro.region_outages_today)                             AS region_outages_today,
    ro.last_outage_cause,
    CASE
        WHEN ZEROIFNULL(ro.region_outages_today) > 0 AND sc.throughput_z < -p.z_limit THEN 'OUTAGE_RELATED'
        WHEN ZEROIFNULL(al.serious_alarms) > 0 AND (sc.throughput_z < -p.z_limit OR sc.drop_rate_z > p.z_limit) THEN 'EQUIPMENT_FAULT'
        WHEN sc.prb_z > p.z_limit AND sc.users_z > 2                THEN 'CONGESTION'
        WHEN sc.latency_z > p.z_limit                               THEN 'TRANSPORT_LATENCY'
        WHEN sc.throughput_z < -p.z_limit OR sc.drop_rate_z > p.z_limit THEN 'UNEXPLAINED'
        ELSE 'NORMAL'
    END                                                             AS anomaly_cause,
    ROUND(
          ABS(LEAST(COALESCE(sc.throughput_z, 0), 0))
        + GREATEST(COALESCE(sc.drop_rate_z, 0), 0)
        + GREATEST(COALESCE(sc.latency_z, 0), 0) * 0.5
    , 3)                                                            AS severity_score,
    RANK() OVER (PARTITION BY sc.run_date, cs.region_id ORDER BY sc.throughput_z NULLS LAST) AS worst_rank_in_region,
    CURRENT_TIMESTAMP()                                             AS loaded_at
FROM scored sc
JOIN params p                ON p.run_date = sc.run_date
LEFT JOIN {{ ref('slv_cell_site') }} cs ON cs.cell_id = sc.cell_id
LEFT JOIN worst_hour wh      ON wh.cell_id = sc.cell_id AND wh.kpi_date = sc.run_date
LEFT JOIN threshold_breach tb ON tb.cell_id = sc.cell_id AND tb.run_date = sc.run_date
LEFT JOIN alarms al          ON al.cell_id = sc.cell_id AND al.run_date = sc.run_date
LEFT JOIN elements el        ON el.cell_id = sc.cell_id
LEFT JOIN home_tickets ht    ON ht.cell_id = sc.cell_id AND ht.run_date = sc.run_date
LEFT JOIN region_outages ro  ON ro.region_id = cs.region_id AND ro.run_date = sc.run_date
