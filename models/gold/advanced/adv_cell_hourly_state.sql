{{
  config(
    materialized = 'dynamic_table',
    target_lag = 'downstream',
    snowflake_warehouse = target.warehouse,
    refresh_mode = 'full',
    on_configuration_change = 'apply',
    tags = ['gold', 'advanced', 'h21', 'network', 'pivot'],
    query_tag = 'dbt_telco_h21'
  )
}}
{#-
  adv_cell_hourly_state  -  Cell daily state from the 24-hour profile
  ------------------------------------------------------------------------------------------
  Ported from x_h21_cell_hourly_pivot_state (PIPE_X.SP_X_H21_CELL_HOURLY_PIVOT_STATE).
  Grain : one row per cell per Manila kpi_date (every day in the loaded window).
  PRB and latency by day part (PIVOT), the 24-hour PRB profile as array and hour->value
  object, p50/p95 latency (PERCENTILE_CONT), degraded hours, normal<->degraded flips
  (CONDITIONAL_CHANGE_EVENT), degraded episodes and the longest one (CONDITIONAL_TRUE_EVENT),
  alarms by severity (PIVOT), threshold-breach hours, regional outages and day-over-day change.

  Changes vs. original:
    * all days in the window (partitioned by cell AND day) instead of one var(run_date);
    * built on silver slv_cell_kpi_hourly (Manila hours, NaN cleaned) instead of re-reading
      315M raw 15-min rows; drop rate is on the % scale there, so DROP_LIMIT is 1.61 (%)
      - the original 0.0161 fraction threshold is the same business rule;
    * the original bucketed hours by the UTC-stored INTERVAL_START date; days are now
      Manila business days.
  Materialization kept as a DYNAMIC TABLE (full refresh, target_lag = downstream), on the
  target warehouse; every input is a silver model because a dynamic table refreshes with
  its owner role only (no secondary roles).

  Snowflake-specific: dynamic table, PIVOT (x3), PERCENTILE_CONT WITHIN GROUP,
  CONDITIONAL_CHANGE_EVENT, CONDITIONAL_TRUE_EVENT, LAG, OBJECT_AGG, ARRAY_AGG WITHIN GROUP,
  LPAD, MAX_BY, MODE, CONVERT_TIMEZONE, DIV0, ZEROIFNULL, IFF.
  Refs (8): slv_cell_kpi_hourly, slv_cell_site, slv_alarm_event,
  slv_cell_kpi_threshold, slv_network_element, slv_outage_incident,
  slv_subscription.
-#}

WITH params AS (
    SELECT
        1.61                                                        AS drop_limit_pct,
        100                                                         AS latency_limit_ms
),

/* ---------- 1. hourly KPIs (silver) ---------- */
hourly_all AS (
    SELECT
        h.cell_id,
        h.hour_local                                                AS hr,
        h.event_date                                                AS kpi_date,
        h.prb_pct_avg                                               AS prb,
        h.latency_ms_max                                            AS lat,
        h.latency_ms_avg                                            AS lat_avg,
        h.drop_rate_pct_avg                                         AS drop_rate_pct,
        h.throughput_mbps_avg                                       AS thr,
        h.active_users_max                                          AS peak_users,
        h.pm_intervals                                              AS intervals
    FROM {{ ref('slv_cell_kpi_hourly') }} h
),

hourly AS (
    SELECT
        h.*,
        HOUR(h.hr)                                                  AS hour_of_day,
        (h.drop_rate_pct > p.drop_limit_pct OR COALESCE(h.lat, 0) > p.latency_limit_ms) AS degraded,
        CASE
            WHEN HOUR(h.hr) < 6  THEN 'NIGHT'
            WHEN HOUR(h.hr) < 12 THEN 'MORNING'
            WHEN HOUR(h.hr) < 18 THEN 'AFTERNOON'
            ELSE 'EVENING'
        END                                                         AS day_part
    FROM hourly_all h
    CROSS JOIN params p
),

states AS (
    SELECT
        h.*,
        CONDITIONAL_CHANGE_EVENT(h.degraded) OVER (PARTITION BY h.cell_id, h.kpi_date ORDER BY h.hr) AS state_changes,
        CONDITIONAL_TRUE_EVENT(h.degraded AND NOT COALESCE(h.prev_degraded, FALSE))
            OVER (PARTITION BY h.cell_id, h.kpi_date ORDER BY h.hr) AS episode_no
    FROM (
        SELECT
            x.*,
            LAG(x.degraded) OVER (PARTITION BY x.cell_id, x.kpi_date ORDER BY x.hr) AS prev_degraded
        FROM hourly x
    ) h
),

episodes AS (
    SELECT
        cell_id,
        kpi_date,
        COUNT(DISTINCT IFF(degraded, episode_no, NULL))             AS degraded_episodes,
        MAX(episode_hours)                                          AS longest_episode_hours
    FROM (
        SELECT
            cell_id,
            kpi_date,
            episode_no,
            degraded,
            COUNT_IF(degraded) OVER (PARTITION BY cell_id, kpi_date, episode_no) AS episode_hours
        FROM states
    )
    GROUP BY cell_id, kpi_date
),

/* ---------- 2. pivots by day part ---------- */
prb_pivot AS (
    SELECT *
    FROM (SELECT cell_id, kpi_date, day_part, prb FROM hourly)
    PIVOT (AVG(prb) FOR day_part IN ('NIGHT', 'MORNING', 'AFTERNOON', 'EVENING'))
        AS pv (cell_id, kpi_date, prb_night, prb_morning, prb_afternoon, prb_evening)
),

latency_pivot AS (
    SELECT *
    FROM (SELECT cell_id, kpi_date, day_part, lat_avg FROM hourly)
    PIVOT (AVG(lat_avg) FOR day_part IN ('NIGHT', 'MORNING', 'AFTERNOON', 'EVENING'))
        AS pv (cell_id, kpi_date, lat_night, lat_morning, lat_afternoon, lat_evening)
),

/* ---------- 3. day statistics and 24-hour profile ---------- */
day_stats AS (
    SELECT
        s.cell_id,
        s.kpi_date,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY s.lat)         AS p95_latency_ms,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY s.lat)         AS p50_latency_ms,
        AVG(s.prb)                                                  AS avg_prb,
        MAX(s.prb)                                                  AS max_prb,
        MAX_BY(s.hour_of_day, s.prb)                                AS busy_hour,
        MAX(s.peak_users)                                           AS peak_users,
        AVG(s.thr)                                                  AS avg_throughput,
        MIN(s.thr)                                                  AS min_throughput,
        COUNT_IF(s.degraded)                                        AS degraded_hours,
        MAX(s.state_changes)                                        AS state_flips,
        COUNT(*)                                                    AS hours_with_data,
        SUM(s.intervals)                                            AS intervals,
        ARRAY_AGG(ROUND(s.prb, 1)) WITHIN GROUP (ORDER BY s.hour_of_day) AS prb_profile_24h,
        OBJECT_AGG(LPAD(s.hour_of_day::STRING, 2, '0'), ROUND(s.prb, 1)::VARIANT) AS prb_by_hour
    FROM states s
    GROUP BY s.cell_id, s.kpi_date
),

prev_day AS (
    SELECT
        h.cell_id,
        DATEADD('day', 1, h.kpi_date)                               AS kpi_date,
        AVG(h.prb)                                                  AS prev_avg_prb,
        COUNT_IF(h.degraded)                                        AS prev_degraded_hours
    FROM hourly h
    GROUP BY h.cell_id, h.kpi_date
),

/* ---------- 4. alarms, thresholds, outages, subscribers ---------- */
alarm_pivot AS (
    SELECT *
    FROM (SELECT cell_id, event_date AS kpi_date, severity, 1 AS n FROM {{ ref('slv_alarm_event') }})
    PIVOT (SUM(n) FOR severity IN ('CRITICAL', 'MAJOR', 'MINOR', 'WARNING'))
        AS pv (cell_id, kpi_date, alarms_critical, alarms_major, alarms_minor, alarms_warning)
),

prb_threshold AS (
    -- silver pivot of NETWORK.CELL_KPI_THRESHOLD (enabled thresholds only). Must be silver:
    -- a dynamic table refreshes as its OWNER role only, which has no grant on the bronze DB.
    SELECT
        cell_id,
        prb_pct_crit                                                AS prb_critical_threshold,
        latency_ms_crit                                             AS latency_critical_threshold
    FROM {{ ref('slv_cell_kpi_threshold') }}
),

threshold_hours AS (
    SELECT
        h.cell_id,
        h.kpi_date,
        COUNT_IF(h.prb > t.prb_critical_threshold)                  AS hours_over_prb_threshold,
        COUNT_IF(h.lat > t.latency_critical_threshold)              AS hours_over_latency_threshold
    FROM hourly h
    JOIN prb_threshold t
      ON t.cell_id = h.cell_id
    GROUP BY h.cell_id, h.kpi_date
),

elements AS (
    SELECT cell_id, MODE(vendor) AS vendor, COUNT(*) AS elements
    FROM {{ ref('slv_network_element') }}
    GROUP BY cell_id
),

days AS (
    SELECT DISTINCT kpi_date FROM day_stats
),

region_outage AS (
    SELECT
        d.kpi_date,
        o.region_id,
        COUNT(*)                                                    AS outages_overlapping_day
    FROM days d
    JOIN {{ ref('slv_outage_incident') }} o
      ON o.detected_at_utc < CONVERT_TIMEZONE('{{ var("local_tz") }}', 'UTC', DATEADD('day', 1, d.kpi_date)::TIMESTAMP_NTZ)
     AND COALESCE(o.restored_at_utc, '9999-12-31'::TIMESTAMP_NTZ)
         >= CONVERT_TIMEZONE('{{ var("local_tz") }}', 'UTC', d.kpi_date::TIMESTAMP_NTZ)
    GROUP BY d.kpi_date, o.region_id
),

home_subs AS (
    SELECT home_cell_id AS cell_id, COUNT(*) AS home_subscribers
    FROM {{ ref('slv_subscription') }}
    WHERE status = 'ACTIVE' AND is_reportable
    GROUP BY home_cell_id
)

SELECT
    ds.kpi_date,
    ds.cell_id,
    c.site_name,
    c.radio_type,
    COALESCE(c.region_name, 'UNKNOWN')                              AS region_name,
    el.vendor,
    ZEROIFNULL(hs.home_subscribers)                                 AS home_subscribers,
    ROUND(pp.prb_night, 2)                                          AS prb_night,
    ROUND(pp.prb_morning, 2)                                        AS prb_morning,
    ROUND(pp.prb_afternoon, 2)                                      AS prb_afternoon,
    ROUND(pp.prb_evening, 2)                                        AS prb_evening,
    ROUND(lp.lat_night, 2)                                          AS latency_night,
    ROUND(lp.lat_morning, 2)                                        AS latency_morning,
    ROUND(lp.lat_afternoon, 2)                                      AS latency_afternoon,
    ROUND(lp.lat_evening, 2)                                        AS latency_evening,
    ROUND(ds.avg_prb, 2)                                            AS avg_prb,
    ROUND(ds.max_prb, 2)                                            AS max_prb,
    ds.busy_hour,
    ds.peak_users,
    ROUND(ds.avg_throughput, 3)                                     AS avg_throughput_mbps,
    ROUND(ds.min_throughput, 3)                                     AS min_throughput_mbps,
    ROUND(ds.p50_latency_ms, 2)                                     AS p50_latency_ms,
    ROUND(ds.p95_latency_ms, 2)                                     AS p95_latency_ms,
    ds.degraded_hours,
    ds.state_flips,
    ZEROIFNULL(ep.degraded_episodes)                                AS degraded_episodes,
    ZEROIFNULL(ep.longest_episode_hours)                            AS longest_episode_hours,
    ds.hours_with_data,
    ds.intervals,
    ds.prb_profile_24h,
    ds.prb_by_hour,
    ROUND(pd.prev_avg_prb, 2)                                       AS prev_day_avg_prb,
    ROUND(ds.avg_prb - pd.prev_avg_prb, 2)                          AS prb_change_vs_prev_day,
    ds.degraded_hours - ZEROIFNULL(pd.prev_degraded_hours)          AS degraded_hours_change,
    ZEROIFNULL(ap.alarms_critical)                                  AS alarms_critical,
    ZEROIFNULL(ap.alarms_major)                                     AS alarms_major,
    ZEROIFNULL(ap.alarms_minor)                                     AS alarms_minor,
    ZEROIFNULL(ap.alarms_warning)                                   AS alarms_warning,
    ZEROIFNULL(ap.alarms_critical) + ZEROIFNULL(ap.alarms_major)
      + ZEROIFNULL(ap.alarms_minor) + ZEROIFNULL(ap.alarms_warning) AS alarms,
    ZEROIFNULL(th.hours_over_prb_threshold)                         AS hours_over_prb_threshold,
    ZEROIFNULL(th.hours_over_latency_threshold)                     AS hours_over_latency_threshold,
    ZEROIFNULL(ro.outages_overlapping_day)                          AS region_outages_overlapping_day,
    CASE
        WHEN ds.degraded_hours >= 12                                THEN 'CHRONIC'
        WHEN ZEROIFNULL(ep.longest_episode_hours) >= 4              THEN 'SUSTAINED'
        WHEN ds.state_flips >= 6                                    THEN 'FLAPPING'
        WHEN ds.degraded_hours > 0                                  THEN 'INTERMITTENT'
        ELSE 'STABLE'
    END                                                             AS day_state,
    IFF(pp.prb_evening > 80 AND ds.peak_users > 0, TRUE, FALSE)     AS evening_congestion,
    ROUND(DIV0(ds.degraded_hours, ds.hours_with_data), 4)           AS degraded_share
FROM day_stats ds
JOIN prb_pivot pp            ON pp.cell_id = ds.cell_id AND pp.kpi_date = ds.kpi_date
LEFT JOIN latency_pivot lp   ON lp.cell_id = ds.cell_id AND lp.kpi_date = ds.kpi_date
LEFT JOIN episodes ep        ON ep.cell_id = ds.cell_id AND ep.kpi_date = ds.kpi_date
LEFT JOIN prev_day pd        ON pd.cell_id = ds.cell_id AND pd.kpi_date = ds.kpi_date
LEFT JOIN alarm_pivot ap     ON ap.cell_id = ds.cell_id AND ap.kpi_date = ds.kpi_date
LEFT JOIN threshold_hours th ON th.cell_id = ds.cell_id AND th.kpi_date = ds.kpi_date
LEFT JOIN elements el        ON el.cell_id = ds.cell_id
LEFT JOIN home_subs hs       ON hs.cell_id = ds.cell_id
LEFT JOIN {{ ref('slv_cell_site') }} c ON c.cell_id = ds.cell_id
LEFT JOIN region_outage ro   ON ro.region_id = c.region_id AND ro.kpi_date = ds.kpi_date
