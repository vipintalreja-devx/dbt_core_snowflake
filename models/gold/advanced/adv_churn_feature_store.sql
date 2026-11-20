{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['subscription_id', 'feature_date'],
    on_schema_change = 'append_new_columns',
    cluster_by = ['feature_date'],
    tags = ['gold', 'advanced', 'h19', 'ml', 'churn'],
    query_tag = 'dbt_telco_h19'
  )
}}
{#-
  adv_churn_feature_store  -  Churn model feature store (month-end snapshots)
  ------------------------------------------------------------------------------------------
  Ported from x_h19_churn_feature_store (PIPE_X.SP_X_H19_CHURN_FEATURE_STORE).
  Grain : one row per subscription_id x feature_date, feature_date = each month end in the
  loaded window; population = lines ACTIVE at that month end (BSS status history).
  ~90 features over 30 / 90 / 180-day windows: usage level + trend (REGR_SLOPE/REGR_R2),
  recharge cadence (LAG gaps), revenue, late fees, dunning, tickets, NPS answers, outages,
  campaign responses, roaming, segments, product context, and the upstream 12-month spend
  arrays from MARKETING.CHURN_FEATURES unpacked with LATERAL FLATTEN.

  Changes vs. original:
    * monthly feature snapshots for every month in the window (merge on line x date)
      instead of one var(run_date); a daily feature store for 440k lines x 90-day windows
      is not viable on an X-Small warehouse and month-end is the model's scoring cadence;
    * population = ACTIVE at month end from status history (point-in-time correct; the
      original used today's status for any run date);
    * windows only see the loaded history (var history_start_date).

  Snowflake-specific: REGR_SLOPE, REGR_R2, STDDEV_SAMP, MEDIAN, LAG, MAX_BY,
  LATERAL FLATTEN over VARIANT arrays, ARRAY_SIZE, VARIANT paths, DIV0, ZEROIFNULL, IFF,
  QUALIFY, HASH.
  Refs/sources (22): int_subscriber_month_status, slv_subscription, slv_account, slv_customer,
  slv_plan, slv_device, slv_contract, slv_partner_brand, fct_subscriber_usage_daily,
  slv_daily_revenue, slv_recharge, slv_late_fee_assessment, slv_dunning_event, slv_ticket,
  slv_nps_survey, slv_outage_impact, slv_outage_incident, source marketing.campaign_response,
  slv_roaming_usage, slv_segment_membership, source marketing.churn_features.
-#}

WITH params AS (
    SELECT
        run_month,
        month_end                                                   AS feature_date,
        DATEADD('day', 1, month_end)                                AS next_day,
        DATEADD('day', -30, month_end)                              AS d30,
        DATEADD('day', -90, month_end)                              AS d90,
        DATEADD('day', -180, month_end)                             AS d180
    FROM ({{ f4_months_cte('feature_date') }})
),

/* ---------- 1. population: lines ACTIVE at the month end ---------- */
population AS (
    SELECT
        p.feature_date,
        s.subscription_id,
        s.account_id,
        s.plan_id,
        s.device_id,
        s.partner_brand_id,
        s.tenure_months,
        s.monthly_charges_php,
        s.total_charges_php,
        s.internet_service,
        s.has_phone_service,
        IFF(s.multiple_lines = 'Yes', 1, 0)                         AS multiple_lines,
        (IFF(s.online_security = 'Yes', 1, 0) + IFF(s.online_backup = 'Yes', 1, 0)
       + IFF(s.device_protection = 'Yes', 1, 0) + IFF(s.tech_support = 'Yes', 1, 0)) AS protection_addons,
        (IFF(s.streaming_tv = 'Yes', 1, 0) + IFF(s.streaming_movies = 'Yes', 1, 0))  AS streaming_addons,
        a.payment_method,
        a.is_paperless_billing,
        c.is_senior,
        c.has_partner,
        c.has_dependents,
        c.preferred_language,
        DATEDIFF('year', c.date_of_birth, p.feature_date)           AS age
    FROM params p
    JOIN {{ ref('int_subscriber_month_status') }} ms
      ON ms.month_start = p.run_month
     AND ms.status_eom = 'ACTIVE'
    JOIN {{ ref('slv_subscription') }} s
      ON s.subscription_id = ms.subscription_id
     AND s.is_reportable
    LEFT JOIN {{ ref('slv_account') }} a  ON a.account_id = s.account_id
    LEFT JOIN {{ ref('slv_customer') }} c ON c.customer_id = a.customer_id
),

/* ---------- 2. usage level and trend ---------- */
usage AS (
    SELECT
        p.feature_date,
        u.subscription_id,
        SUM(IFF(u.event_date > p.d30, u.data_mb, 0))                AS data_mb_30d,
        SUM(u.data_mb)                                              AS data_mb_90d,
        SUM(IFF(u.event_date > p.d30, u.voice_min, 0))              AS voice_min_30d,
        SUM(u.voice_min)                                            AS voice_min_90d,
        SUM(IFF(u.event_date > p.d30, u.sms_count, 0))              AS sms_30d,
        COUNT_IF(u.event_date > p.d30)                              AS active_days_30d,
        COUNT(*)                                                    AS active_days_90d,
        REGR_SLOPE(u.data_mb, DATEDIFF('day', p.d90, u.event_date)) AS data_trend_slope,
        REGR_R2(u.data_mb, DATEDIFF('day', p.d90, u.event_date))    AS data_trend_r2,
        REGR_SLOPE(u.voice_min, DATEDIFF('day', p.d90, u.event_date)) AS voice_trend_slope,
        STDDEV_SAMP(u.data_mb)                                      AS data_mb_stddev,
        MAX(u.event_date)                                           AS last_usage_date
    FROM params p
    JOIN {{ ref('fct_subscriber_usage_daily') }} u
      ON u.event_date >  p.d90
     AND u.event_date <= p.feature_date
    GROUP BY p.feature_date, u.subscription_id
),

/* ---------- 3. money ---------- */
revenue AS (
    SELECT
        p.feature_date,
        r.subscription_id,
        SUM(IFF(r.business_date > p.d30, r.revenue_php, 0))         AS revenue_30d,
        SUM(r.revenue_php)                                          AS revenue_90d,
        SUM(IFF(r.revenue_source = 'VAS', r.revenue_php, 0))        AS vas_revenue_90d
    FROM params p
    JOIN {{ ref('slv_daily_revenue') }} r
      ON r.business_date >  p.d90
     AND r.business_date <= p.feature_date
    GROUP BY p.feature_date, r.subscription_id
),

recharge_events AS (
    SELECT
        rc.subscription_id,
        rc.recharged_at_local,
        rc.event_date,
        rc.recharge_amount_php,
        rc.recharge_channel,
        DATEDIFF('day', LAG(rc.recharged_at_local) OVER (PARTITION BY rc.subscription_id ORDER BY rc.recharged_at_local),
                 rc.recharged_at_local)                             AS days_since_prev
    FROM {{ ref('slv_recharge') }} rc
    WHERE rc.event_date >= (SELECT MIN(d180) FROM params)
      AND rc.event_date <  (SELECT MAX(next_day) FROM params)
),

recharges AS (
    SELECT
        p.feature_date,
        re.subscription_id,
        COUNT_IF(re.event_date >= p.d30)                            AS recharges_30d,
        SUM(IFF(re.event_date >= p.d30, re.recharge_amount_php, 0)) AS recharge_php_30d,
        COUNT(*)                                                    AS recharges_180d,
        AVG(re.days_since_prev)                                     AS avg_days_between_recharges,
        MEDIAN(re.days_since_prev)                                  AS median_days_between_recharges,
        MAX(re.days_since_prev)                                     AS max_recharge_gap_days,
        DATEDIFF('day', MAX(re.event_date), p.feature_date)         AS days_since_last_recharge,
        COUNT(DISTINCT re.recharge_channel)                         AS recharge_channels
    FROM params p
    JOIN recharge_events re
      ON re.event_date >= p.d180
     AND re.event_date <  p.next_day
    GROUP BY p.feature_date, re.subscription_id
),

late_fees AS (
    SELECT
        p.feature_date,
        lf.account_id,
        COUNT(*)                                                    AS late_fees_180d,
        SUM(lf.fee_php)                                             AS late_fee_php_180d,
        MAX(lf.days_overdue)                                        AS max_days_overdue_180d
    FROM params p
    JOIN {{ ref('slv_late_fee_assessment') }} lf
      ON lf.assessment_date >  p.d180
     AND lf.assessment_date <= p.feature_date
    GROUP BY p.feature_date, lf.account_id
),

dunning AS (
    SELECT
        p.feature_date,
        d.account_id,
        MAX(d.dunning_stage)                                        AS max_dunning_stage_180d,
        COUNT_IF(d.dunning_action = 'SERVICE_BARRED')               AS barrings_180d
    FROM params p
    JOIN {{ ref('slv_dunning_event') }} d
      ON d.action_date >= p.d180
     AND d.action_date <  p.next_day
    GROUP BY p.feature_date, d.account_id
),

/* ---------- 4. experience ---------- */
tickets AS (
    SELECT
        p.feature_date,
        t.subscription_id,
        COUNT(*)                                                    AS tickets_90d,
        COUNT_IF(t.opened_date >= p.d30)                            AS tickets_30d,
        COUNT_IF(t.category IN ('NO_INTERNET', 'SLOW_SPEED'))       AS network_tickets_90d,
        COUNT_IF(t.category = 'BILLING')                            AS billing_tickets_90d,
        COUNT_IF(t.category = 'PORTING')                            AS porting_tickets_90d,
        COUNT_IF(t.priority = 'P1')                                 AS p1_tickets_90d,
        COUNT_IF(t.is_open)                                         AS open_tickets,
        AVG(t.resolution_hours)                                     AS avg_resolution_hours
    FROM params p
    JOIN {{ ref('slv_ticket') }} t
      ON t.opened_date >= p.d90
     AND t.opened_date <  p.next_day
    GROUP BY p.feature_date, t.subscription_id
),

nps AS (
    SELECT
        p.feature_date,
        n.subscription_id,
        MAX_BY(n.nps_score, n.responded_at_utc)                     AS last_nps,
        MAX_BY(n.q_price, n.responded_at_utc)                       AS last_price_rating,
        MAX_BY(n.q_coverage, n.responded_at_utc)                    AS last_coverage_rating,
        MAX_BY(n.q_support, n.responded_at_utc)                     AS last_support_rating,
        DATEDIFF('day', MAX(n.response_date), p.feature_date)       AS days_since_nps
    FROM params p
    JOIN {{ ref('slv_nps_survey') }} n
      ON n.response_date < p.next_day
    GROUP BY p.feature_date, n.subscription_id
),

outages AS (
    SELECT
        p.feature_date,
        im.subscription_id,
        SUM(im.impact_minutes)                                      AS outage_min_90d,
        COUNT(DISTINCT im.incident_id)                              AS outages_90d
    FROM params p
    JOIN {{ ref('slv_outage_incident') }} i
      ON i.event_date >  p.d90
     AND i.event_date <= p.feature_date
    JOIN {{ ref('slv_outage_impact') }} im
      ON im.incident_id = i.incident_id
    GROUP BY p.feature_date, im.subscription_id
),

/* ---------- 5. marketing and roaming ---------- */
campaigns AS (
    SELECT
        p.feature_date,
        cr.subscription_id,
        COUNT(*)                                                    AS campaigns_received_90d,
        COUNT_IF(cr.opened_at IS NOT NULL)                          AS campaigns_opened_90d,
        COUNT_IF(cr.converted)                                      AS campaigns_converted_90d,
        MAX_BY(cr.response_payload:device::STRING, cr.sent_at)      AS last_response_device
    FROM params p
    JOIN {{ source('marketing', 'campaign_response') }} cr
      ON cr.sent_at >= p.d90::TIMESTAMP_LTZ
     AND cr.sent_at <  p.next_day::TIMESTAMP_LTZ
    GROUP BY p.feature_date, cr.subscription_id
),

roaming AS (
    SELECT
        p.feature_date,
        ru.subscription_id,
        COUNT(DISTINCT ru.event_date)                               AS roaming_days_90d,
        SUM(ru.charge_php_billed)                                   AS roaming_php_90d
    FROM params p
    JOIN {{ ref('slv_roaming_usage') }} ru
      ON ru.event_date >  p.d90
     AND ru.event_date <= p.feature_date
    GROUP BY p.feature_date, ru.subscription_id
),

segments AS (
    SELECT
        p.feature_date,
        sm.subscription_id,
        MAX(IFF(sm.segment_code = 'AT_RISK', 1, 0))                 AS seg_at_risk,
        MAX(IFF(sm.segment_code = 'VALUE_HIGH', 1, 0))              AS seg_value_high,
        MAX(IFF(sm.segment_code = 'DATA_HEAVY', 1, 0))              AS seg_data_heavy,
        MAX(IFF(sm.segment_code = 'VOICE_LOYAL', 1, 0))             AS seg_voice_loyal
    FROM params p
    JOIN {{ ref('slv_segment_membership') }} sm
      ON p.feature_date >= sm.valid_from
     AND p.feature_date <  sm.valid_to
    GROUP BY p.feature_date, sm.subscription_id
),

/* ---------- 6. upstream feature arrays (12 monthly values) ---------- */
upstream AS (
    SELECT
        p.feature_date,
        cf.subscription_id,
        cf.feature_date                                             AS upstream_feature_date,
        cf.avg_monthly_spend,
        cf.data_trend_pct,
        cf.late_payments_6m,
        cf.features:last_recharge_days::NUMBER                      AS upstream_last_recharge_days,
        cf.features:tickets_open::NUMBER                            AS upstream_tickets_open,
        ARRAY_SIZE(cf.features:top_cells)                           AS upstream_top_cells,
        cf.features                                                 AS features
    FROM params p
    JOIN {{ source('marketing', 'churn_features') }} cf
      ON cf.feature_date <= p.feature_date
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.feature_date, cf.subscription_id ORDER BY cf.feature_date DESC) = 1
),

upstream_arrays AS (
    SELECT
        up.feature_date,
        up.subscription_id,
        AVG(IFF(f.index >= 9, f.value::FLOAT, NULL))                AS spend_last_3m_avg,
        AVG(IFF(f.index < 3, f.value::FLOAT, NULL))                 AS spend_first_3m_avg,
        MAX(f.value::FLOAT)                                         AS spend_12m_max,
        MIN(f.value::FLOAT)                                         AS spend_12m_min
    FROM upstream up,
    LATERAL FLATTEN(INPUT => up.features:spend_php_12m) f
    GROUP BY up.feature_date, up.subscription_id
),

/* ---------- 7. product context ---------- */
context AS (
    SELECT
        pp.feature_date,
        pp.subscription_id,
        pc.plan_name,
        pc.contract_type                                            AS plan_contract_type,
        pc.base_price_php,
        DIV0(pp.monthly_charges_php, pc.base_price_php)             AS price_vs_list,
        dc.manufacturer,
        COALESCE(dc.supports_5g, FALSE)                             AS device_5g,
        DATEDIFF('month', dc.launch_date, pp.feature_date)          AS device_age_months,
        ct.contract_type,
        ct.is_auto_renew,
        DATEDIFF('month', pp.feature_date, ct.end_date)             AS contract_months_left,
        pb.brand_name                                               AS partner_brand
    FROM population pp
    LEFT JOIN {{ ref('slv_plan') }} pc
           ON pc.plan_id = pp.plan_id
    LEFT JOIN {{ ref('slv_device') }} dc
           ON dc.device_id = pp.device_id
    LEFT JOIN {{ ref('slv_contract') }} ct
           ON ct.subscription_id = pp.subscription_id
          AND ct.start_date <= pp.feature_date
          AND (ct.terminated_on IS NULL OR ct.terminated_on > pp.feature_date)
    LEFT JOIN {{ ref('slv_partner_brand') }} pb
           ON pb.partner_brand_id = pp.partner_brand_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pp.feature_date, pp.subscription_id
                               ORDER BY ct.start_date DESC NULLS LAST) = 1
)

SELECT
    pp.subscription_id,
    pp.feature_date,
    pp.account_id,
    pp.plan_id,
    cx.plan_name,
    cx.plan_contract_type,
    pp.tenure_months,
    pp.monthly_charges_php,
    pp.total_charges_php,
    ROUND(cx.price_vs_list, 4)                                      AS price_vs_list,
    pp.internet_service,
    pp.has_phone_service,
    pp.multiple_lines,
    pp.protection_addons,
    pp.streaming_addons,
    pp.payment_method,
    pp.is_paperless_billing,
    pp.is_senior,
    pp.has_partner,
    pp.has_dependents,
    pp.preferred_language,
    pp.age,
    ZEROIFNULL(u.data_mb_30d)                                       AS data_mb_30d,
    ZEROIFNULL(u.data_mb_90d)                                       AS data_mb_90d,
    ROUND(DIV0(u.data_mb_30d, u.data_mb_90d / 3), 4)                AS data_30d_vs_90d_avg,
    ROUND(u.data_trend_slope, 4)                                    AS data_trend_slope,
    ROUND(u.data_trend_r2, 4)                                       AS data_trend_r2,
    ROUND(u.voice_trend_slope, 4)                                   AS voice_trend_slope,
    ZEROIFNULL(u.voice_min_30d)                                     AS voice_min_30d,
    ZEROIFNULL(u.voice_min_90d)                                     AS voice_min_90d,
    ZEROIFNULL(u.sms_30d)                                           AS sms_30d,
    ZEROIFNULL(u.active_days_30d)                                   AS active_days_30d,
    ZEROIFNULL(u.active_days_90d)                                   AS active_days_90d,
    ROUND(DIV0(u.data_mb_stddev, DIV0(u.data_mb_90d, u.active_days_90d)), 4) AS data_volatility,
    DATEDIFF('day', u.last_usage_date, pp.feature_date)             AS days_since_usage,
    ZEROIFNULL(r.revenue_30d)                                       AS revenue_30d,
    ZEROIFNULL(r.revenue_90d)                                       AS revenue_90d,
    ROUND(DIV0(r.vas_revenue_90d, r.revenue_90d), 4)                AS vas_share_90d,
    ZEROIFNULL(rc.recharges_30d)                                    AS recharges_30d,
    ZEROIFNULL(rc.recharge_php_30d)                                 AS recharge_php_30d,
    ZEROIFNULL(rc.recharges_180d)                                   AS recharges_180d,
    ROUND(rc.avg_days_between_recharges, 2)                         AS avg_days_between_recharges,
    rc.median_days_between_recharges,
    rc.max_recharge_gap_days,
    rc.days_since_last_recharge,
    IFF(rc.days_since_last_recharge > 2 * rc.median_days_between_recharges, 1, 0) AS recharge_overdue,
    ZEROIFNULL(rc.recharge_channels)                                AS recharge_channels,
    ZEROIFNULL(lf.late_fees_180d)                                   AS late_fees_180d,
    ZEROIFNULL(lf.late_fee_php_180d)                                AS late_fee_php_180d,
    lf.max_days_overdue_180d,
    ZEROIFNULL(dn.max_dunning_stage_180d)                           AS max_dunning_stage_180d,
    ZEROIFNULL(dn.barrings_180d)                                    AS barrings_180d,
    ZEROIFNULL(t.tickets_30d)                                       AS tickets_30d,
    ZEROIFNULL(t.tickets_90d)                                       AS tickets_90d,
    ZEROIFNULL(t.network_tickets_90d)                               AS network_tickets_90d,
    ZEROIFNULL(t.billing_tickets_90d)                               AS billing_tickets_90d,
    ZEROIFNULL(t.porting_tickets_90d)                               AS porting_tickets_90d,
    ZEROIFNULL(t.p1_tickets_90d)                                    AS p1_tickets_90d,
    ZEROIFNULL(t.open_tickets)                                      AS open_tickets,
    ROUND(t.avg_resolution_hours, 1)                                AS avg_resolution_hours,
    n.last_nps,
    n.last_price_rating,
    n.last_coverage_rating,
    n.last_support_rating,
    n.days_since_nps,
    ZEROIFNULL(o.outage_min_90d)                                    AS outage_min_90d,
    ZEROIFNULL(o.outages_90d)                                       AS outages_90d,
    ZEROIFNULL(cp.campaigns_received_90d)                           AS campaigns_received_90d,
    ROUND(DIV0(cp.campaigns_opened_90d, cp.campaigns_received_90d), 4) AS campaign_open_rate_90d,
    ZEROIFNULL(cp.campaigns_converted_90d)                          AS campaigns_converted_90d,
    cp.last_response_device,
    ZEROIFNULL(ro.roaming_days_90d)                                 AS roaming_days_90d,
    ZEROIFNULL(ro.roaming_php_90d)                                  AS roaming_php_90d,
    ZEROIFNULL(sg.seg_at_risk)                                      AS seg_at_risk,
    ZEROIFNULL(sg.seg_value_high)                                   AS seg_value_high,
    ZEROIFNULL(sg.seg_data_heavy)                                   AS seg_data_heavy,
    ZEROIFNULL(sg.seg_voice_loyal)                                  AS seg_voice_loyal,
    cx.manufacturer                                                 AS device_manufacturer,
    cx.device_5g,
    cx.device_age_months,
    cx.contract_type,
    cx.is_auto_renew,
    cx.contract_months_left,
    IFF(cx.contract_months_left BETWEEN 0 AND 2, 1, 0)              AS contract_ending_soon,
    cx.partner_brand,
    up.upstream_feature_date,
    up.avg_monthly_spend                                            AS upstream_avg_monthly_spend,
    up.data_trend_pct                                               AS upstream_data_trend_pct,
    up.late_payments_6m                                             AS upstream_late_payments_6m,
    up.upstream_last_recharge_days,
    up.upstream_tickets_open,
    up.upstream_top_cells,
    ROUND(ua.spend_last_3m_avg, 2)                                  AS spend_last_3m_avg,
    ROUND(ua.spend_first_3m_avg, 2)                                 AS spend_first_3m_avg,
    ROUND(DIV0(ua.spend_last_3m_avg - ua.spend_first_3m_avg, ua.spend_first_3m_avg), 4) AS spend_12m_change,
    ROUND(ua.spend_12m_max - ua.spend_12m_min, 2)                   AS spend_12m_range,
    HASH(pp.subscription_id, pp.feature_date)                       AS feature_row_key,
    CURRENT_TIMESTAMP()                                             AS loaded_at
FROM population pp
LEFT JOIN usage u            ON u.subscription_id = pp.subscription_id  AND u.feature_date = pp.feature_date
LEFT JOIN revenue r          ON r.subscription_id = pp.subscription_id  AND r.feature_date = pp.feature_date
LEFT JOIN recharges rc       ON rc.subscription_id = pp.subscription_id AND rc.feature_date = pp.feature_date
LEFT JOIN late_fees lf       ON lf.account_id = pp.account_id           AND lf.feature_date = pp.feature_date
LEFT JOIN dunning dn         ON dn.account_id = pp.account_id           AND dn.feature_date = pp.feature_date
LEFT JOIN tickets t          ON t.subscription_id = pp.subscription_id  AND t.feature_date = pp.feature_date
LEFT JOIN nps n              ON n.subscription_id = pp.subscription_id  AND n.feature_date = pp.feature_date
LEFT JOIN outages o          ON o.subscription_id = pp.subscription_id  AND o.feature_date = pp.feature_date
LEFT JOIN campaigns cp       ON cp.subscription_id = pp.subscription_id AND cp.feature_date = pp.feature_date
LEFT JOIN roaming ro         ON ro.subscription_id = pp.subscription_id AND ro.feature_date = pp.feature_date
LEFT JOIN segments sg        ON sg.subscription_id = pp.subscription_id AND sg.feature_date = pp.feature_date
LEFT JOIN upstream up        ON up.subscription_id = pp.subscription_id AND up.feature_date = pp.feature_date
LEFT JOIN upstream_arrays ua ON ua.subscription_id = pp.subscription_id AND ua.feature_date = pp.feature_date
LEFT JOIN context cx         ON cx.subscription_id = pp.subscription_id AND cx.feature_date = pp.feature_date
