-- =============================================================================
-- 09_bi.sql -- BI consumers: Tableau and Power BI service users running tagged dashboard
-- queries on a schedule, through the regional analyst roles (so row-level security applies).
--   users  SVC_TABLEAU (TELCO_ANALYST_NCR) / SVC_POWERBI (TELCO_ANALYST_VISMIN), TYPE = SERVICE,
--          key-pair only, BI_WH, no secondary roles
--   schema BI_SIM: one caller's-rights procedure per tool (dashboard queries + QUERY_TAG)
--          and tasks owned by the analyst role that EXECUTE AS USER the service user
-- Public keys below; private keys live outside the repo (~/.ssh/snowflake/svc_*_key.p8).
-- =============================================================================
USE ROLE USERADMIN;
CREATE USER IF NOT EXISTS SVC_TABLEAU TYPE = SERVICE
  DEFAULT_ROLE = TELCO_ANALYST_NCR DEFAULT_WAREHOUSE = BI_WH DEFAULT_SECONDARY_ROLES = ()
  RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsLBztZNZ1NziXRpShBq9S9X4XaAGQ84qQoQ3q4QmDbR9Z/NG9C4xXSGQTCz8z2q/C/fBcC2Zu6kTJsbSeevPRTvud5SRJ21mZreVDQur+qs+/4QM4wn3KMG2gzwxpUFYXwE4dvD8VQf0fUbgOD1xJ6l4aLBcJLVv3w2LrVPhCdeB/IWLAXZg+T/0pHcVPHw8iEpVPlHXqxSWfRCNvznKAry56UUDZ+Seu/IJknDxDWdvh+A5F3qoN9aWrFROHY6xp2yR0RA2qQYR47p5CRCIBl5xS1UAX2DQJRUyRLwm542gocu6mvTHnrjGT8TxDuMEFHpFc5FaGOjpV5ji1/1R/wIDAQAB'
  COMMENT = 'Tableau Server extracts / live dashboards (NCR analytics team)';
CREATE USER IF NOT EXISTS SVC_POWERBI TYPE = SERVICE
  DEFAULT_ROLE = TELCO_ANALYST_VISMIN DEFAULT_WAREHOUSE = BI_WH DEFAULT_SECONDARY_ROLES = ()
  RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqhq/MCmS27n98AdLtMSTEs2ptSrwV7bjnynww4IrgJNb9Dek4oqURtzJKtt4/at4HVf/RvAri3MhyiD62FPiQ2oEms6M2mh+xz0Y43pF7gkLfVMhliPY/Bwu/QDX0OnhAoygTkih6vSHbmAEfaURuOq3aetFoEZHk49d1rb1Az4LCgOUvABeG56sM/oEojU3cJCBXC2eJsux7iv1ak0fUgkUMqlHO731HRkZ97XUSdssm2yJ5/1FjE72Lmq57Lv5yQAOvg3YuAKdeb3mt2io+sjLOVz3DXqBiJXPdASDRiA6LyGPBES3ffsa6GtOoUGniaGOALQzXHTolCXlfXhjzQIDAQAB'
  COMMENT = 'Power BI gateway (Visayas-Mindanao analytics team)';

USE ROLE SECURITYADMIN;
GRANT ROLE TELCO_ANALYST_NCR    TO USER SVC_TABLEAU;
GRANT ROLE TELCO_ANALYST_VISMIN TO USER SVC_POWERBI;

USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE TELCO_ANALYST_NCR;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE TELCO_ANALYST_VISMIN;
GRANT IMPERSONATE ON USER SVC_TABLEAU TO ROLE TELCO_ANALYST_NCR;
GRANT IMPERSONATE ON USER SVC_POWERBI TO ROLE TELCO_ANALYST_VISMIN;

USE ROLE SYSADMIN;
USE DATABASE TELCO_PROD_XL_V2;
CREATE SCHEMA IF NOT EXISTS BI_SIM COMMENT = 'Simulated BI tools: dashboard procedures + refresh schedules';
GRANT USAGE, CREATE PROCEDURE, CREATE TASK ON SCHEMA BI_SIM TO ROLE TELCO_ANALYST_NCR;
GRANT USAGE, CREATE PROCEDURE, CREATE TASK ON SCHEMA BI_SIM TO ROLE TELCO_ANALYST_VISMIN;

-- ---------------------------------------------------------------- Tableau (NCR)
USE ROLE TELCO_ANALYST_NCR;
CREATE OR REPLACE PROCEDURE BI_SIM.SP_TABLEAU_REFRESH()
RETURNS VARCHAR LANGUAGE SQL
COMMENT = 'Tableau dashboards: Executive KPI, Churn, Network Quality, Outages, Revenue Trend, Cell Anomalies'
EXECUTE AS CALLER
AS
$$
DECLARE n INTEGER DEFAULT 0; c INTEGER;
BEGIN
  ALTER SESSION SET QUERY_TAG = 'bi:tableau:executive_kpi';
  SELECT COUNT(*) INTO :c FROM (SELECT month_start, region_name, SUM(revenue_php) revenue, SUM(revenue_php) / NULLIF(SUM(base_avg), 0) arpu
    FROM TELCO_PROD_XL_V2.DBT_TELCO_GOLD.MART_ARPU_MONTHLY WHERE month_start >= DATEADD('month', -6, CURRENT_DATE()) GROUP BY 1, 2); n := n + c;
  ALTER SESSION SET QUERY_TAG = 'bi:tableau:churn';
  SELECT COUNT(*) INTO :c FROM (SELECT month_start, region_name, contract_type, churn_rate_pct, churned_lines, net_adds
    FROM TELCO_PROD_XL_V2.DBT_TELCO_GOLD.MART_CHURN_MONTHLY WHERE month_start >= DATEADD('month', -6, CURRENT_DATE())); n := n + c;
  ALTER SESSION SET QUERY_TAG = 'bi:tableau:network_quality';
  SELECT COUNT(*) INTO :c FROM (SELECT event_date, region_name, pct_cells_degraded, drop_rate_pct_avg, throughput_mbps_avg, outages
    FROM TELCO_PROD_XL_V2.DBT_TELCO_GOLD.MART_NETWORK_QUALITY_DAILY WHERE event_date >= DATEADD('day', -14, CURRENT_DATE())); n := n + c;
  ALTER SESSION SET QUERY_TAG = 'bi:tableau:outages';
  SELECT COUNT(*) INTO :c FROM (SELECT region_name, severity, root_cause, COUNT(*) incidents, SUM(outage_minutes) minutes, SUM(impacted_lines) lines
    FROM TELCO_PROD_XL_V2.DBT_TELCO_GOLD.FCT_OUTAGE WHERE event_date >= DATEADD('day', -30, CURRENT_DATE()) GROUP BY 1, 2, 3); n := n + c;
  ALTER SESSION SET QUERY_TAG = 'bi:tableau:revenue_trend';
  SELECT COUNT(*) INTO :c FROM (SELECT r.business_date, s.home_region_name, SUM(r.total_revenue_php) revenue
    FROM TELCO_PROD_XL_V2.DBT_TELCO_GOLD.FCT_REVENUE_DAILY r JOIN TELCO_PROD_XL_V2.DBT_TELCO_GOLD.DIM_SUBSCRIBER s ON s.subscription_id = r.subscription_id
    WHERE r.business_date >= DATEADD('day', -30, CURRENT_DATE()) GROUP BY 1, 2); n := n + c;
  ALTER SESSION SET QUERY_TAG = 'bi:tableau:cell_anomalies';
  SELECT COUNT(*) INTO :c FROM (SELECT kpi_date, region_name, site_name, anomaly_cause, severity_score
    FROM TELCO_PROD_XL_V2.DBT_TELCO_GOLD.ADV_CELL_ANOMALY WHERE kpi_date >= DATEADD('day', -7, CURRENT_DATE()) AND is_robust_anomaly); n := n + c;
  ALTER SESSION UNSET QUERY_TAG;
  RETURN 'tableau refresh: 6 dashboards, ' || n || ' rows';
END;
$$;

CREATE OR REPLACE TASK BI_SIM.T_TABLEAU_REFRESH
  WAREHOUSE = BI_WH
  SCHEDULE = 'USING CRON 0 7,12,17 * * * Asia/Manila'
  COMMENT = 'Tableau extract refresh as SVC_TABLEAU (role TELCO_ANALYST_NCR)'
AS CALL BI_SIM.SP_TABLEAU_REFRESH();
ALTER TASK BI_SIM.T_TABLEAU_REFRESH SET EXECUTE AS USER SVC_TABLEAU;
ALTER TASK BI_SIM.T_TABLEAU_REFRESH RESUME;

-- ---------------------------------------------------------------- Power BI (VisMin)
USE ROLE TELCO_ANALYST_VISMIN;
CREATE OR REPLACE PROCEDURE BI_SIM.SP_POWERBI_REFRESH()
RETURNS VARCHAR LANGUAGE SQL
COMMENT = 'Power BI datasets: AR Aging, Collections, Campaigns, ARPU Tiers, Care SLA, Cell KPIs'
EXECUTE AS CALLER
AS
$$
DECLARE n INTEGER DEFAULT 0; c INTEGER;
BEGIN
  ALTER SESSION SET QUERY_TAG = 'bi:powerbi:ar_aging';
  SELECT COUNT(*) INTO :c FROM (SELECT month_end, aging_bucket, open_invoices, outstanding_php
    FROM TELCO_PROD_XL_V2.DBT_TELCO_GOLD.MART_AR_AGING_MONTHLY WHERE month_end >= DATEADD('month', -6, CURRENT_DATE())); n := n + c;
  ALTER SESSION SET QUERY_TAG = 'bi:powerbi:collections';
  SELECT COUNT(*) INTO :c FROM (SELECT payment_date, payment_channel, COUNT(*) payments, SUM(payment_amount_php) amount, COUNT_IF(is_late) late
    FROM TELCO_PROD_XL_V2.DBT_TELCO_GOLD.FCT_PAYMENT WHERE payment_date >= DATEADD('day', -30, CURRENT_DATE()) GROUP BY 1, 2); n := n + c;
  ALTER SESSION SET QUERY_TAG = 'bi:powerbi:campaigns';
  SELECT COUNT(*) INTO :c FROM (SELECT send_month, channel, campaign_name, conversion_rate, uplift_roi
    FROM TELCO_PROD_XL_V2.DBT_TELCO_GOLD.ANL_CAMPAIGN_CONVERSION_MONTHLY WHERE send_month >= DATEADD('month', -3, CURRENT_DATE())); n := n + c;
  ALTER SESSION SET QUERY_TAG = 'bi:powerbi:arpu_tiers';
  SELECT COUNT(*) INTO :c FROM (SELECT revenue_month, region_name, arpu_php, arpu_tier, arpu_rank
    FROM TELCO_PROD_XL_V2.DBT_TELCO_GOLD.ANL_ARPU_REGION_TIER_MONTHLY); n := n + c;
  ALTER SESSION SET QUERY_TAG = 'bi:powerbi:care_sla';
  SELECT COUNT(*) INTO :c FROM (SELECT opened_date, category, priority, sla_attainment_pct, avg_nps_score
    FROM TELCO_PROD_XL_V2.DBT_TELCO_GOLD.MART_CARE_SLA_DAILY WHERE opened_date >= DATEADD('day', -30, CURRENT_DATE())); n := n + c;
  ALTER SESSION SET QUERY_TAG = 'bi:powerbi:cell_kpis';
  SELECT COUNT(*) INTO :c FROM (SELECT k.event_date, c.region_name, COUNT_IF(k.is_degraded) degraded_cells, AVG(k.drop_rate_pct_avg) drop_rate
    FROM TELCO_PROD_XL_V2.DBT_TELCO_GOLD.FCT_CELL_KPI_DAILY k JOIN TELCO_PROD_XL_V2.DBT_TELCO_GOLD.DIM_CELL c ON c.cell_id = k.cell_id
    WHERE k.event_date >= DATEADD('day', -14, CURRENT_DATE()) GROUP BY 1, 2); n := n + c;
  ALTER SESSION UNSET QUERY_TAG;
  RETURN 'powerbi refresh: 6 datasets, ' || n || ' rows';
END;
$$;

CREATE OR REPLACE TASK BI_SIM.T_POWERBI_REFRESH
  WAREHOUSE = BI_WH
  SCHEDULE = 'USING CRON 30 8,13 * * * Asia/Manila'
  COMMENT = 'Power BI dataset refresh as SVC_POWERBI (role TELCO_ANALYST_VISMIN)'
AS CALL BI_SIM.SP_POWERBI_REFRESH();
ALTER TASK BI_SIM.T_POWERBI_REFRESH SET EXECUTE AS USER SVC_POWERBI;
ALTER TASK BI_SIM.T_POWERBI_REFRESH RESUME;
