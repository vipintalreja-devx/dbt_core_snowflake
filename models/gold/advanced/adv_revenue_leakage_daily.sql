{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'usage_date',
    on_schema_change = 'append_new_columns',
    cluster_by = ['usage_date', 'leakage_type'],
    tags = ['gold', 'advanced', 'h18', 'revenue_assurance'],
    query_tag = 'dbt_telco_h18'
  )
}}
{#-
  adv_revenue_leakage_daily  -  Revenue assurance: rated vs booked, per line per day
  ------------------------------------------------------------------------------------------
  Ported from x_h18_revenue_leakage_daily (PIPE_X.SP_X_H18_REVENUE_LEAKAGE_DAILY).
  Grain : one row per usage_date (Manila) x subscription_id with anything rated, booked or
  failed in mediation.
  Compares what the network rated (voice + SMS CDRs, mediation xDRs, roaming) with the
  non-plan revenue booked, and flags duplicated CDRs, late CDRs, failed mediation records,
  unknown / deleted subscribers and usage that an unlimited pack should have zero-rated.

  Design note: revenue assurance must see the RAW evidence that silver deliberately cleans
  away (duplicates, late arrivals, FAILED xDRs), so voice / SMS / xDR are read from bronze
  sources; everything else comes from silver/gold.
  Changes vs. original: every Manila day in the window (incremental, lookback re-processed);
  late = ingested on/after the second local day after the event (same rule, date-based);
  data MB uses silver (binary MB, Manila day - the original bucketed UTC dates).

  Snowflake-specific: CONVERT_TIMEZONE, TO_TIMESTAMP_NTZ(epoch, 3), VARIANT paths,
  TRY_TO_NUMBER, COUNT(*) OVER for duplicate detection, FULL OUTER JOIN chain, BOOLOR_AGG,
  ARRAY_AGG DISTINCT, QUALIFY, IFF, DIV0, ZEROIFNULL, GREATEST, RATIO_TO_REPORT, RANK.
  Refs/sources (13): source usage.cdr_voice, source usage.cdr_sms, source usage.xdr_mediation_raw,
  slv_cdr_data, slv_roaming_usage, slv_recharge, slv_daily_revenue, slv_subscription,
  slv_plan, source ref.plan_feature, slv_account, slv_partner_brand, dim_date (spine).
-#}

WITH params AS (
    SELECT
        d.run_date,
        d.next_date,
        1.00                                                        AS abs_tolerance_php,
        0.05                                                        AS rel_tolerance
    FROM ({{ f4_days_cte('usage_date') }}) d
),

bounds AS (
    SELECT
        MIN(run_date)                                               AS min_date,
        MAX(next_date)                                              AS max_next_date,
        DATEADD('day', -1, MIN(run_date))::TIMESTAMP_NTZ::TIMESTAMP_TZ AS min_ts,
        DATEADD('day',  1, MAX(next_date))::TIMESTAMP_NTZ::TIMESTAMP_TZ AS max_ts
    FROM params
),

/* ---------- 1. raw voice CDRs per Manila day, with duplicate and late flags ---------- */
voice_raw AS (
    SELECT
        CONVERT_TIMEZONE('{{ var("local_tz") }}', c.call_start)::DATE AS run_date,
        c.call_id,
        c.subscription_id,
        c.call_start,
        c.duration_sec,
        c.call_type,
        COALESCE(c.is_roaming, FALSE)                               AS is_roaming,
        ZEROIFNULL(c.charge_php)                                    AS charge_php,
        CONVERT_TIMEZONE('{{ var("local_tz") }}', c.ingested_at)::DATE AS ingested_date,
        COUNT(*) OVER (PARTITION BY c.subscription_id, c.call_start, c.duration_sec) AS same_call_rows
    FROM {{ source('usage', 'cdr_voice') }} c
    WHERE c.call_start >= (SELECT min_ts FROM bounds)
      AND c.call_start <  (SELECT max_ts FROM bounds)
),

voice AS (
    SELECT
        v.run_date,
        v.subscription_id,
        COUNT(*)                                                    AS calls,
        SUM(v.duration_sec)                                         AS call_seconds,
        SUM(IFF(v.same_call_rows = 1, v.charge_php, v.charge_php / v.same_call_rows)) AS voice_php,
        SUM(IFF(v.is_roaming, v.charge_php, 0))                     AS voice_roaming_php,
        SUM(IFF(v.call_type = 'MO', v.charge_php, 0))               AS voice_mo_php,
        COUNT_IF(v.same_call_rows > 1)                              AS duplicate_call_rows,
        SUM(IFF(v.same_call_rows > 1, v.charge_php * (v.same_call_rows - 1) / v.same_call_rows, 0)) AS duplicate_voice_php,
        COUNT_IF(v.ingested_date >= DATEADD('day', 2, v.run_date))  AS late_calls,
        COUNT_IF(v.charge_php = 0 AND v.duration_sec > 0 AND v.call_type = 'MO') AS zero_rated_mo_calls
    FROM voice_raw v
    JOIN params p ON p.run_date = v.run_date
    GROUP BY v.run_date, v.subscription_id
),

/* ---------- 2. SMS (raw, for late detection) ---------- */
sms AS (
    SELECT
        CONVERT_TIMEZONE('{{ var("local_tz") }}', m.sent_at)::DATE AS run_date,
        m.subscription_id,
        COUNT(*)                                                    AS sms_count,
        SUM(ZEROIFNULL(m.charge_php))                               AS sms_php,
        SUM(IFF(m.sms_type = 'PREMIUM', ZEROIFNULL(m.charge_php), 0)) AS premium_sms_php,
        COUNT_IF(m.sms_type = 'A2P')                                AS a2p_sms,
        COUNT_IF(CONVERT_TIMEZONE('{{ var("local_tz") }}', m.ingested_at)::DATE
                 >= DATEADD('day', 2, CONVERT_TIMEZONE('{{ var("local_tz") }}', m.sent_at)::DATE)) AS late_sms
    FROM {{ source('usage', 'cdr_sms') }} m
    WHERE m.sent_at >= (SELECT min_ts FROM bounds)
      AND m.sent_at <  (SELECT max_ts FROM bounds)
    GROUP BY 1, 2
    HAVING run_date IN (SELECT run_date FROM params)
),

/* ---------- 3. data sessions (volume only, charged inside the plan) ---------- */
data_sessions AS (
    SELECT
        d.event_date                                                AS run_date,
        d.subscription_id,
        COUNT(*)                                                    AS data_sessions,
        ROUND(SUM(d.data_mb), 3)                                    AS data_mb,
        COUNT_IF(d.is_roaming)                                      AS roaming_data_sessions
    FROM {{ ref('slv_cdr_data') }} d
    WHERE d.event_date IN (SELECT run_date FROM params)
    GROUP BY 1, 2
),

/* ---------- 4. mediation records (raw): parse, split good and failed ---------- */
xdr_parsed AS (
    SELECT
        x.record_id,
        x.source_system,
        x.parse_status,
        x.payload:sub_id::NUMBER                                    AS subscription_id,
        x.payload:msisdn::STRING                                    AS msisdn,
        TRY_TO_NUMBER(x.payload:charging.amount_php::STRING, 12, 4) AS charge_php,
        x.payload:charging.rating_group::NUMBER                     AS rating_group,
        CONVERT_TIMEZONE('UTC', '{{ var("local_tz") }}',
            TO_TIMESTAMP_NTZ(x.payload:start_epoch_ms::NUMBER, 3))::DATE AS event_date_mnl
    FROM {{ source('usage', 'xdr_mediation_raw') }} x
    WHERE x.received_at >= DATEADD('day', -1, (SELECT min_date FROM bounds))::TIMESTAMP_LTZ
      AND x.received_at <  DATEADD('day', 3, (SELECT max_next_date FROM bounds))::TIMESTAMP_LTZ
),

xdr AS (
    SELECT
        xp.event_date_mnl                                           AS run_date,
        COALESCE(xp.subscription_id, s.subscription_id)             AS subscription_id,
        COUNT_IF(xp.parse_status = 'OK')                            AS xdr_records,
        SUM(IFF(xp.parse_status = 'OK', ZEROIFNULL(xp.charge_php), 0)) AS xdr_php,
        COUNT_IF(xp.parse_status <> 'OK')                           AS xdr_failed,
        SUM(IFF(xp.parse_status <> 'OK', ZEROIFNULL(xp.charge_php), 0)) AS xdr_failed_php,
        ARRAY_AGG(DISTINCT xp.source_system)                        AS xdr_sources
    FROM xdr_parsed xp
    LEFT JOIN {{ ref('slv_subscription') }} s
           ON s.msisdn = {{ normalize_msisdn('xp.msisdn') }}
          AND xp.subscription_id IS NULL
    WHERE xp.event_date_mnl IN (SELECT run_date FROM params)
    GROUP BY 1, 2
),

/* ---------- 5. roaming usage and recharges ---------- */
roaming AS (
    SELECT
        ru.event_date                                               AS run_date,
        ru.subscription_id,
        SUM(ru.charge_php_billed)                                   AS roaming_php,
        SUM(ru.charge_usd)                                          AS roaming_usd
    FROM {{ ref('slv_roaming_usage') }} ru
    WHERE ru.event_date IN (SELECT run_date FROM params)
    GROUP BY 1, 2
),

recharges AS (
    SELECT
        p.run_date,
        rc.subscription_id,
        COUNT(*)                                                    AS recharges,
        SUM(rc.recharge_amount_php)                                 AS recharge_php,
        BOOLOR_AGG(rc.pack_code IN ('UNLI_CALL', 'COMBO_7D'))       AS has_unli_voice_pack,
        SUM(IFF(rc.event_date = p.run_date, rc.recharge_amount_php, 0)) AS recharge_today_php
    FROM params p
    JOIN {{ ref('slv_recharge') }} rc
      ON rc.event_date >= DATEADD('day', -7, p.run_date)
     AND rc.event_date <  p.next_date
    GROUP BY 1, 2
),

/* ---------- 6. revenue booked for the day, by source ---------- */
booked AS (
    SELECT
        r.business_date                                             AS run_date,
        r.subscription_id,
        SUM(IFF(r.revenue_source <> 'PLAN',     r.revenue_php, 0))  AS booked_non_plan_php,
        SUM(IFF(r.revenue_source = 'VAS',       r.revenue_php, 0))  AS booked_vas_php,
        SUM(IFF(r.revenue_source = 'ROAMING',   r.revenue_php, 0))  AS booked_roaming_php,
        SUM(IFF(r.revenue_source = 'RECHARGE',  r.revenue_php, 0))  AS booked_recharge_php,
        SUM(IFF(r.revenue_source = 'PLAN',      r.revenue_php, 0))  AS booked_plan_php
    FROM {{ ref('slv_daily_revenue') }} r
    WHERE r.business_date IN (SELECT run_date FROM params)
    GROUP BY 1, 2
),

/* ---------- 7. all lines with anything rated (FULL OUTER JOIN chain) ---------- */
rated AS (
    SELECT
        COALESCE(v.run_date, m.run_date, x.run_date, ro.run_date)   AS run_date,
        COALESCE(v.subscription_id, m.subscription_id, x.subscription_id, ro.subscription_id) AS subscription_id,
        ZEROIFNULL(v.calls)                                         AS calls,
        ZEROIFNULL(v.call_seconds)                                  AS call_seconds,
        ZEROIFNULL(v.voice_php)                                     AS voice_php,
        ZEROIFNULL(v.voice_roaming_php)                             AS voice_roaming_php,
        ZEROIFNULL(v.duplicate_call_rows)                           AS duplicate_call_rows,
        ZEROIFNULL(v.duplicate_voice_php)                           AS duplicate_voice_php,
        ZEROIFNULL(v.late_calls)                                    AS late_calls,
        ZEROIFNULL(v.zero_rated_mo_calls)                           AS zero_rated_mo_calls,
        ZEROIFNULL(m.sms_count)                                     AS sms_count,
        ZEROIFNULL(m.sms_php)                                       AS sms_php,
        ZEROIFNULL(m.premium_sms_php)                               AS premium_sms_php,
        ZEROIFNULL(m.late_sms)                                      AS late_sms,
        ZEROIFNULL(x.xdr_records)                                   AS xdr_records,
        ZEROIFNULL(x.xdr_php)                                       AS xdr_php,
        ZEROIFNULL(x.xdr_failed)                                    AS xdr_failed,
        ZEROIFNULL(x.xdr_failed_php)                                AS xdr_failed_php,
        x.xdr_sources,
        ZEROIFNULL(ro.roaming_php)                                  AS roaming_php
    FROM voice v
    FULL OUTER JOIN sms m
      ON m.subscription_id = v.subscription_id
     AND m.run_date = v.run_date
    FULL OUTER JOIN xdr x
      ON x.subscription_id = COALESCE(v.subscription_id, m.subscription_id)
     AND x.run_date = COALESCE(v.run_date, m.run_date)
    FULL OUTER JOIN roaming ro
      ON ro.subscription_id = COALESCE(v.subscription_id, m.subscription_id, x.subscription_id)
     AND ro.run_date = COALESCE(v.run_date, m.run_date, x.run_date)
),

subscriber AS (
    SELECT
        s.subscription_id,
        s.account_id,
        s.plan_id,
        s.status,
        s.is_deleted,
        pc.plan_name,
        pc.contract_type,
        COALESCE(pf.is_included, FALSE)                             AS plan_has_unli_calls,
        a.payment_method,
        pb.brand_name                                               AS partner_brand
    FROM {{ ref('slv_subscription') }} s
    LEFT JOIN {{ ref('slv_plan') }} pc
           ON pc.plan_id = s.plan_id
    LEFT JOIN {{ source('ref', 'plan_feature') }} pf
           ON pf.plan_id = s.plan_id
          AND pf.feature_code = 'CALLS'
          AND CURRENT_DATE BETWEEN pf.effective_from AND COALESCE(pf.effective_to, '9999-12-31'::DATE)
    LEFT JOIN {{ ref('slv_account') }} a
           ON a.account_id = s.account_id
    LEFT JOIN {{ ref('slv_partner_brand') }} pb
           ON pb.partner_brand_id = s.partner_brand_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.subscription_id ORDER BY pf.effective_from DESC NULLS LAST) = 1
),

compared AS (
    SELECT
        r.*,
        sb.account_id,
        sb.plan_id,
        sb.plan_name,
        sb.contract_type,
        sb.status,
        sb.payment_method,
        sb.partner_brand,
        IFF(sb.subscription_id IS NULL, TRUE, FALSE)                AS unknown_subscriber,
        COALESCE(sb.is_deleted, FALSE)                              AS deleted_subscriber,
        COALESCE(sb.plan_has_unli_calls, FALSE) OR COALESCE(rc.has_unli_voice_pack, FALSE) AS unli_voice,
        ZEROIFNULL(ds.data_sessions)                                AS data_sessions,
        ZEROIFNULL(ds.data_mb)                                      AS data_mb,
        ZEROIFNULL(rc.recharge_today_php)                           AS recharge_today_php,
        ZEROIFNULL(bk.booked_non_plan_php)                          AS booked_php,
        ZEROIFNULL(bk.booked_vas_php)                               AS booked_vas_php,
        ZEROIFNULL(bk.booked_roaming_php)                           AS booked_roaming_php,
        ZEROIFNULL(bk.booked_recharge_php)                          AS booked_recharge_php,
        ZEROIFNULL(bk.booked_plan_php)                              AS booked_plan_php,
        r.voice_php + r.sms_php + r.xdr_php                         AS rated_php
    FROM rated r
    LEFT JOIN subscriber sb       ON sb.subscription_id = r.subscription_id
    LEFT JOIN data_sessions ds    ON ds.subscription_id = r.subscription_id AND ds.run_date = r.run_date
    LEFT JOIN recharges rc        ON rc.subscription_id = r.subscription_id AND rc.run_date = r.run_date
    LEFT JOIN booked bk           ON bk.subscription_id = r.subscription_id AND bk.run_date = r.run_date
)

SELECT
    c.run_date                                                      AS usage_date,
    c.subscription_id,
    c.account_id,
    c.plan_id,
    c.plan_name,
    c.contract_type,
    c.status,
    c.payment_method,
    c.partner_brand,
    c.calls,
    c.call_seconds,
    ROUND(c.voice_php, 4)                                           AS voice_php,
    ROUND(c.voice_roaming_php, 4)                                   AS voice_roaming_php,
    c.sms_count,
    ROUND(c.sms_php, 4)                                             AS sms_php,
    ROUND(c.premium_sms_php, 4)                                     AS premium_sms_php,
    c.xdr_records,
    ROUND(c.xdr_php, 4)                                             AS xdr_php,
    c.xdr_failed,
    ROUND(c.xdr_failed_php, 4)                                      AS xdr_failed_php,
    c.xdr_sources,
    ROUND(c.roaming_php, 4)                                         AS roaming_feed_php,
    c.data_sessions,
    c.data_mb,
    ROUND(c.recharge_today_php, 2)                                  AS recharge_today_php,
    ROUND(c.rated_php, 4)                                           AS rated_php,
    ROUND(c.booked_php, 4)                                          AS booked_php,
    ROUND(c.booked_vas_php, 4)                                      AS booked_vas_php,
    ROUND(c.booked_roaming_php, 4)                                  AS booked_roaming_php,
    ROUND(c.booked_recharge_php, 4)                                 AS booked_recharge_php,
    ROUND(c.rated_php - c.booked_php, 4)                            AS gap_php,
    ROUND(c.roaming_php - c.booked_roaming_php, 4)                  AS roaming_gap_php,
    ROUND(DIV0(c.rated_php - c.booked_php, c.rated_php), 4)         AS gap_ratio,
    c.duplicate_call_rows,
    ROUND(c.duplicate_voice_php, 4)                                 AS duplicate_voice_php,
    c.late_calls + c.late_sms                                       AS late_records,
    c.zero_rated_mo_calls,
    c.unli_voice,
    c.unknown_subscriber,
    c.deleted_subscriber,
    ABS(c.rated_php - c.booked_php) > GREATEST(p.abs_tolerance_php, p.rel_tolerance * c.rated_php) AS is_leakage,
    CASE
        WHEN c.unknown_subscriber                                   THEN 'UNKNOWN_SUBSCRIBER'
        WHEN c.deleted_subscriber AND c.rated_php > 0               THEN 'DELETED_BUT_RATED'
        WHEN c.duplicate_call_rows > 0                              THEN 'DUPLICATE_CDR'
        WHEN c.xdr_failed > 0 AND c.xdr_failed_php > 0              THEN 'MEDIATION_FAILURE'
        WHEN c.unli_voice AND c.voice_php > 0                       THEN 'UNLI_CHARGED'
        WHEN NOT c.unli_voice AND c.zero_rated_mo_calls > 0         THEN 'MISSED_RATING'
        WHEN c.rated_php - c.booked_php > GREATEST(p.abs_tolerance_php, p.rel_tolerance * c.rated_php) THEN 'UNDER_BOOKED'
        WHEN c.booked_php - c.rated_php > GREATEST(p.abs_tolerance_php, p.rel_tolerance * c.rated_php) THEN 'OVER_BOOKED'
        ELSE 'OK'
    END                                                             AS leakage_type,
    ROUND(GREATEST(c.rated_php - c.booked_php, 0) + c.xdr_failed_php, 4) AS amount_at_stake_php,
    ROUND(RATIO_TO_REPORT(GREATEST(c.rated_php - c.booked_php, 0)) OVER (PARTITION BY c.run_date), 8) AS share_of_day_gap,
    RANK() OVER (PARTITION BY c.run_date ORDER BY ABS(c.rated_php - c.booked_php) DESC, c.subscription_id) AS gap_rank,
    CURRENT_TIMESTAMP()                                             AS loaded_at
FROM compared c
JOIN params p ON p.run_date = c.run_date
WHERE c.rated_php > 0
   OR c.booked_php > 0
   OR c.xdr_failed > 0
