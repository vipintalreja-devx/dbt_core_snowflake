{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'subscription_id',
    merge_exclude_columns = ['first_seen_at'],
    on_schema_change = 'sync_all_columns',
    cluster_by = ['status', 'region_name'],
    post_hook = [
      "delete from {{ this }} where subscription_id in (select subscription_id from {{ ref('slv_subscription') }} where is_deleted)",
      "{{ msisdn_mask_apply(['msisdn']) }}"
    ],
    tags = ['gold', 'advanced', 'h16', 'cdc', 'scd1', 'subscriber'],
    query_tag = 'dbt_telco_h16'
  )
}}
{#-
  adv_subscription_current_scd1  -  Current wide subscription state (SCD Type 1)
  ------------------------------------------------------------------------------------------
  Ported from x_h16_subscription_cdc_scd1 (PIPE_X.SP_X_H16_SUBSCRIPTION_CDC_SCD1 - was a
  stream with SHOW_INITIAL_ROWS + MERGE that also deleted rows).
  Grain : one row per subscription_id (soft-deleted lines and test customers excluded).
  Incremental MERGE keyed on subscription_id; first_seen_at survives re-merges
  (merge_exclude_columns); a post-hook deletes lines soft-deleted since the last run.
  State covers status history, plan + features, device, SIM, contract, partner brand,
  service address / FTTH node, home region, consents, last activity, churn score,
  segments and a change hash.

  Change vs. original: as-of day = the last loaded usage day (data-driven) instead of
  var(run_date); reads silver (normalised MSISDN joins SIM inventory reliably, Manila-local
  timestamps, deduplicated contracts / consents).

  Snowflake-specific: MAX_BY, OBJECT_AGG, BOOLAND_AGG, ARRAY_AGG ... WITHIN GROUP,
  ARRAY_CONTAINS, ARRAY_CONSTRUCT, HASH, LISTAGG DISTINCT, EQUAL_NULL, QUALIFY, IFF,
  ZEROIFNULL, merge_exclude_columns, post-hook DELETE.
  Refs/sources (19): slv_subscription, slv_subscription_status_history, slv_account,
  slv_customer, slv_customer_consent, slv_plan, source ref.plan_feature, slv_device,
  slv_sim_inventory, slv_contract, slv_partner_brand, slv_service_instance, slv_cell_site,
  fct_subscriber_usage_daily, slv_recharge, slv_payment, slv_ticket, slv_churn_score,
  slv_segment_membership.
-#}

WITH params AS (
    SELECT
        run_day,
        DATEADD('day', 1, run_day)                                  AS next_day,
        DATEADD('day', -90, run_day)                                AS lookback_start
    FROM (SELECT MAX(event_date) AS run_day FROM {{ ref('fct_subscriber_usage_daily') }})
),

/* ---------- 1. the lines (soft-deleted ones excluded) ---------- */
subs AS (
    SELECT *
    FROM {{ ref('slv_subscription') }}
    WHERE NOT is_deleted
),

status_now AS (
    SELECT
        h.subscription_id,
        MAX_BY(h.status, h.valid_from_local)                        AS history_status,
        MAX(h.valid_from_local)                                     AS status_since,
        COUNT(*)                                                    AS status_changes,
        COUNT_IF(h.status = 'SUSPENDED')                            AS times_suspended
    FROM {{ ref('slv_subscription_status_history') }} h
    CROSS JOIN params p
    WHERE h.valid_from_date < p.next_day
    GROUP BY h.subscription_id
),

/* ---------- 2. account and customer ---------- */
account_customer AS (
    SELECT
        a.account_id,
        a.customer_id,
        a.payment_method,
        a.is_paperless_billing,
        a.billing_cycle_day,
        c.customer_code,
        c.gender,
        c.is_senior,
        c.has_partner,
        c.has_dependents,
        c.preferred_language,
        c.kyc_doc_type,
        c.kyc_has_address_proof,
        c.kyc_liveness_score,
        DATEDIFF('year', c.date_of_birth, p.run_day)                AS age,
        c.is_test_customer
    FROM {{ ref('slv_account') }} a
    CROSS JOIN params p
    LEFT JOIN {{ ref('slv_customer') }} c
           ON c.customer_id = a.customer_id
),

consents AS (
    SELECT
        cc.customer_id,
        OBJECT_AGG(cc.consent_type, cc.is_granted::VARIANT)         AS consents,
        BOOLAND_AGG(cc.is_granted)                                  AS all_consents_granted,
        MAX(cc.captured_at_utc)                                     AS last_consent_at_utc
    FROM {{ ref('slv_customer_consent') }} cc
    GROUP BY cc.customer_id
),

/* ---------- 3. plan, device, SIM, contract, partner ---------- */
plan_features AS (
    SELECT
        pf.plan_id,
        ARRAY_AGG(pf.feature_code) WITHIN GROUP (ORDER BY pf.feature_code) AS features
    FROM {{ source('ref', 'plan_feature') }} pf
    CROSS JOIN params p
    WHERE pf.is_included
      AND p.run_day BETWEEN pf.effective_from AND COALESCE(pf.effective_to, '9999-12-31'::DATE)
    GROUP BY pf.plan_id
),

sim AS (
    SELECT msisdn, iccid, sim_type, sim_status, activated_at_utc AS sim_activated_at_utc
    FROM {{ ref('slv_sim_inventory') }}
    WHERE msisdn IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY msisdn ORDER BY activated_at_utc DESC NULLS LAST) = 1
),

contract_now AS (
    SELECT
        ct.subscription_id,
        ct.contract_id,
        ct.contract_type,
        ct.start_date                                               AS contract_start,
        ct.end_date                                                 AS contract_end,
        ct.is_auto_renew
    FROM {{ ref('slv_contract') }} ct
    CROSS JOIN params p
    WHERE ct.start_date <= p.run_day
      AND (ct.terminated_on IS NULL OR ct.terminated_on > p.run_day)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ct.subscription_id ORDER BY ct.start_date DESC) = 1
),

/* ---------- 4. where the service is ---------- */
service AS (
    SELECT
        subscription_id,
        service_type,
        ont_serial,
        ftth_node_id,
        city                                                        AS service_city,
        barangay                                                    AS service_barangay,
        zip_code                                                    AS service_zip
    FROM {{ ref('slv_service_instance') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY is_active DESC, provisioned_at_utc DESC) = 1
),

/* ---------- 5. last activity (90 days) ---------- */
last_usage AS (
    SELECT
        u.subscription_id,
        MAX(u.event_date)                                           AS last_usage_date,
        SUM(u.data_mb)                                              AS data_mb_90d,
        COUNT(*)                                                    AS active_days_90d
    FROM {{ ref('fct_subscriber_usage_daily') }} u
    CROSS JOIN params p
    WHERE u.event_date >  p.lookback_start
      AND u.event_date <= p.run_day
    GROUP BY u.subscription_id
),

last_recharge AS (
    SELECT
        rc.subscription_id,
        MAX(rc.recharged_at_local)                                  AS last_recharge_at,
        MAX_BY(rc.pack_code, rc.recharged_at_local)                 AS last_pack
    FROM {{ ref('slv_recharge') }} rc
    CROSS JOIN params p
    WHERE rc.event_date >= p.lookback_start
      AND rc.event_date <  p.next_day
    GROUP BY rc.subscription_id
),

last_payment AS (
    SELECT
        pm.account_id,
        MAX(pm.paid_at_local)                                       AS last_payment_at,
        MAX_BY(pm.payment_amount_php, pm.paid_at_local)             AS last_payment_php
    FROM {{ ref('slv_payment') }} pm
    CROSS JOIN params p
    WHERE pm.payment_date < p.next_day
    GROUP BY pm.account_id
),

last_ticket AS (
    SELECT
        t.subscription_id,
        MAX(t.opened_at_local)                                      AS last_ticket_at,
        MAX_BY(t.category, t.opened_at_local)                       AS last_ticket_category,
        COUNT_IF(t.is_open)                                         AS open_tickets
    FROM {{ ref('slv_ticket') }} t
    CROSS JOIN params p
    WHERE t.opened_date < p.next_day
    GROUP BY t.subscription_id
),

segments AS (
    SELECT
        sm.subscription_id,
        LISTAGG(DISTINCT sm.segment_code, '|') WITHIN GROUP (ORDER BY sm.segment_code) AS segments
    FROM {{ ref('slv_segment_membership') }} sm
    CROSS JOIN params p
    WHERE p.run_day >= sm.valid_from AND p.run_day < sm.valid_to
    GROUP BY sm.subscription_id
),

/* ---------- 6. assemble the current state ---------- */
state AS (
    SELECT
        s.subscription_id,
        s.account_id,
        ac.customer_id,
        ac.customer_code,
        s.msisdn,
        s.status,
        sn.status_since,
        DATEDIFF('day', sn.status_since, p.run_day)                 AS days_in_status,
        IFF(NOT EQUAL_NULL(s.status, sn.history_status), TRUE, FALSE) AS status_mismatch,
        ZEROIFNULL(sn.status_changes)                               AS status_changes,
        ZEROIFNULL(sn.times_suspended)                              AS times_suspended,
        s.activated_at_utc,
        s.activation_date                                           AS activated_date_mnl,
        s.deactivation_date,
        s.tenure_months,
        s.monthly_charges_php,
        s.total_charges_php,
        s.plan_id,
        pn.plan_name,
        pn.contract_type                                            AS plan_contract_type,
        pn.base_price_php,
        pn.speed_mbps                                               AS plan_speed_mbps,
        pn.data_cap_gb                                              AS plan_data_cap_gb,
        pf.features                                                 AS plan_features,
        ARRAY_CONTAINS('ROAMING'::VARIANT, COALESCE(pf.features, ARRAY_CONSTRUCT())) AS has_roaming_feature,
        s.has_phone_service,
        s.multiple_lines,
        s.internet_service,
        (IFF(s.online_security = 'Yes', 1, 0) + IFF(s.online_backup = 'Yes', 1, 0)
       + IFF(s.device_protection = 'Yes', 1, 0) + IFF(s.tech_support = 'Yes', 1, 0)
       + IFF(s.streaming_tv = 'Yes', 1, 0) + IFF(s.streaming_movies = 'Yes', 1, 0)) AS addon_count,
        dv.manufacturer                                             AS device_manufacturer,
        dv.model                                                    AS device_model,
        dv.device_type,
        dv.supports_5g                                              AS device_5g,
        DATEDIFF('month', dv.launch_date, p.run_day)                AS device_age_months,
        sm.iccid,
        sm.sim_type,
        sm.sim_status,
        ct.contract_id,
        ct.contract_type,
        ct.contract_start,
        ct.contract_end,
        ct.is_auto_renew,
        DATEDIFF('day', p.run_day, ct.contract_end)                 AS contract_days_left,
        s.partner_brand_id,
        pb.brand_name                                               AS partner_brand,
        pb.operator_type                                            AS partner_type,
        sv.service_type,
        sv.ftth_node_id,
        sv.ont_serial,
        sv.service_city,
        sv.service_barangay,
        sv.service_zip,
        s.home_cell_id,
        hm.radio_type                                               AS home_radio_type,
        COALESCE(hm.region_name, 'UNKNOWN')                         AS region_name,
        ac.payment_method,
        ac.is_paperless_billing,
        ac.billing_cycle_day,
        ac.gender,
        ac.is_senior,
        ac.has_partner,
        ac.has_dependents,
        ac.preferred_language,
        ac.age,
        ac.kyc_doc_type,
        ac.kyc_has_address_proof,
        ac.kyc_liveness_score,
        cn.consents,
        COALESCE(cn.all_consents_granted, FALSE)                    AS all_consents_granted,
        lu.last_usage_date,
        DATEDIFF('day', lu.last_usage_date, p.run_day)              AS days_since_usage,
        ZEROIFNULL(lu.data_mb_90d)                                  AS data_mb_90d,
        ZEROIFNULL(lu.active_days_90d)                              AS active_days_90d,
        lr.last_recharge_at,
        lr.last_pack,
        lp.last_payment_at,
        lp.last_payment_php,
        lt.last_ticket_at,
        lt.last_ticket_category,
        ZEROIFNULL(lt.open_tickets)                                 AS open_tickets,
        ch.churn_probability,
        ch.score_date                                               AS churn_score_date,
        sg.segments,
        COALESCE(ac.is_test_customer, FALSE)                        AS is_test_customer
    FROM subs s
    CROSS JOIN params p
    LEFT JOIN status_now sn        ON sn.subscription_id = s.subscription_id
    LEFT JOIN account_customer ac  ON ac.account_id = s.account_id
    LEFT JOIN consents cn          ON cn.customer_id = ac.customer_id
    LEFT JOIN {{ ref('slv_plan') }} pn ON pn.plan_id = s.plan_id
    LEFT JOIN plan_features pf     ON pf.plan_id = s.plan_id
    LEFT JOIN {{ ref('slv_device') }} dv ON dv.device_id = s.device_id
    LEFT JOIN sim sm               ON sm.msisdn = s.msisdn
    LEFT JOIN contract_now ct      ON ct.subscription_id = s.subscription_id
    LEFT JOIN {{ ref('slv_partner_brand') }} pb ON pb.partner_brand_id = s.partner_brand_id
    LEFT JOIN service sv           ON sv.subscription_id = s.subscription_id
    LEFT JOIN {{ ref('slv_cell_site') }} hm ON hm.cell_id = s.home_cell_id
    LEFT JOIN last_usage lu        ON lu.subscription_id = s.subscription_id
    LEFT JOIN last_recharge lr     ON lr.subscription_id = s.subscription_id
    LEFT JOIN last_payment lp      ON lp.account_id = s.account_id
    LEFT JOIN last_ticket lt       ON lt.subscription_id = s.subscription_id
    LEFT JOIN {{ ref('slv_churn_score') }} ch ON ch.subscription_id = s.subscription_id
    LEFT JOIN segments sg          ON sg.subscription_id = s.subscription_id
)

SELECT
    st.*,
    CASE
        WHEN st.status = 'CHURNED'                                  THEN 'GONE'
        WHEN st.status = 'SUSPENDED'                                THEN 'AT_RISK'
        WHEN st.days_since_usage > 30 OR st.last_usage_date IS NULL THEN 'DORMANT'
        WHEN COALESCE(st.churn_probability, 0) >= {{ var('churn_high_threshold') }} THEN 'AT_RISK'
        ELSE 'HEALTHY'
    END                                                             AS lifecycle_state,
    HASH(st.status, st.plan_id, st.monthly_charges_php, st.device_model, st.sim_type,
         st.contract_id, st.partner_brand_id, st.service_zip, st.segments) AS state_hash,
    CURRENT_TIMESTAMP()                                             AS first_seen_at,
    CURRENT_TIMESTAMP()                                             AS loaded_at
FROM state st
WHERE NOT st.is_test_customer
QUALIFY ROW_NUMBER() OVER (PARTITION BY st.subscription_id ORDER BY st.contract_start DESC NULLS LAST) = 1
