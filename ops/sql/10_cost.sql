-- =============================================================================
-- 10_cost.sql -- daily credit cap for the pipeline and BI warehouses.
-- Steady state is ~1-2 credits/day (one daily graph run on Small + 5 BI refreshes on X-Small).
-- Attached after the one-off backfill so the backfill was not capped.
-- =============================================================================
USE ROLE ACCOUNTADMIN;
CREATE RESOURCE MONITOR IF NOT EXISTS TELCO_DAILY_RM
  WITH CREDIT_QUOTA = 10 FREQUENCY = DAILY START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 75 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND
           ON 110 PERCENT DO SUSPEND_IMMEDIATE;
ALTER WAREHOUSE ETL_WH SET RESOURCE_MONITOR = TELCO_DAILY_RM;
ALTER WAREHOUSE BI_WH  SET RESOURCE_MONITOR = TELCO_DAILY_RM;
-- the one-off backfill/rebuild ran on MEDIUM; steady state is SMALL
ALTER WAREHOUSE ETL_WH SET WAREHOUSE_SIZE = 'SMALL';
