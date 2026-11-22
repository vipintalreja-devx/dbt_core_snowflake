-- =============================================================================
-- 08_tasks.sql -- the daily task graph (owner DBT_ROLE, warehouse ETL_WH).
--
--   T_ROOT 01:10 Asia/Manila
--     └ T_INGEST          INGEST.SP_INGEST_DAY(yesterday)      source systems write inserts/updates/deletes/late rows
--        └ T_LOAD_FILES   INGEST.SP_LOAD_FILES(yesterday)      Snowpipe refresh + wait + file/row reconciliation
--           ├ T_NATIVE    OPS.SP_RUN_NATIVE(yesterday)         client's PIPE / PIPE_X / MART procedures
--           └ T_FRESHNESS dbt source freshness                  stop before publishing stale data
--              └ T_DBT_BUILD (after T_NATIVE + T_FRESHNESS)  dbt build: seeds, CDC silver, snapshots, gold, tests
--                 └ T_QA  OPS.SP_QA_CHECKS(yesterday)          reconciliation + RLS coverage; fails the graph on FAIL
--   T_FINALIZE (finalizer of T_ROOT, runs even on failure)     OPS.PIPELINE_RUN_LOG row per task + graph status
-- =============================================================================
USE ROLE DBT_ROLE;
USE DATABASE TELCO_PROD_XL_V2;
USE SCHEMA OPS;

CREATE TABLE IF NOT EXISTS OPS.PIPELINE_RUN_LOG (
  GRAPH_RUN_GROUP_ID VARCHAR, BUSINESS_DATE DATE, TASK_NAME VARCHAR, STATE VARCHAR,
  STARTED_AT TIMESTAMP_LTZ, COMPLETED_AT TIMESTAMP_LTZ, SECONDS NUMBER(10,1), ERROR_MESSAGE VARCHAR,
  RETURN_VALUE VARCHAR, LOGGED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP());

CREATE OR REPLACE PROCEDURE OPS.SP_FINALIZE()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Task-graph finalizer: one OPS.PIPELINE_RUN_LOG row per task of the current graph run; raises an error when any task failed'
EXECUTE AS CALLER
AS
$$
DECLARE
  graph_failed EXCEPTION (-20001, 'telco_daily graph FAILED: one or more tasks failed or were cancelled; see OPS.PIPELINE_RUN_LOG');
  gid VARCHAR;
  bdate DATE DEFAULT DATEADD('day', -1, CONVERT_TIMEZONE('Asia/Manila', CURRENT_TIMESTAMP())::DATE);
  n_failed INTEGER;
  n_total INTEGER;
  failures VARCHAR;
  dbt_secs NUMBER(10,1);
BEGIN
  gid := (SELECT SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_RUN_GROUP_ID'));
  INSERT INTO OPS.PIPELINE_RUN_LOG (GRAPH_RUN_GROUP_ID, BUSINESS_DATE, TASK_NAME, STATE, STARTED_AT, COMPLETED_AT, SECONDS, ERROR_MESSAGE, RETURN_VALUE)
  SELECT graph_run_group_id, :bdate, name, state, query_start_time, completed_time,
         ROUND(DATEDIFF('millisecond', query_start_time, completed_time) / 1000, 1), error_message, LEFT(return_value, 1000)
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
           SCHEDULED_TIME_RANGE_START => DATEADD('hour', -12, CURRENT_TIMESTAMP()), RESULT_LIMIT => 1000))
   WHERE graph_run_group_id = :gid AND database_name = 'TELCO_PROD_XL_V2' AND schema_name = 'OPS'
     AND name <> 'T_FINALIZE';
  SELECT COUNT_IF(state IN ('FAILED', 'CANCELLED')), COUNT(*) INTO :n_failed, :n_total
    FROM OPS.PIPELINE_RUN_LOG WHERE graph_run_group_id = :gid;
  INSERT INTO OPS.PIPELINE_RUN_LOG (GRAPH_RUN_GROUP_ID, BUSINESS_DATE, TASK_NAME, STATE, RETURN_VALUE)
  VALUES (:gid, :bdate, 'GRAPH', IFF(:n_failed = 0, 'SUCCEEDED', 'FAILED'), :n_total || ' tasks, ' || :n_failed || ' failed');
  -- Slack: COMPLETED or FAILED (with the failing tasks); the notifier never fails the finalizer
  SELECT COALESCE(LISTAGG(task_name || ': ' || LEFT(REGEXP_REPLACE(COALESCE(error_message, state), '\\x1b\\[[0-9;]*m', ''), 160), '\n'), '')
    INTO :failures FROM OPS.PIPELINE_RUN_LOG
   WHERE graph_run_group_id = :gid AND task_name <> 'GRAPH' AND state IN ('FAILED', 'CANCELLED');
  SELECT COALESCE(MAX(IFF(task_name = 'T_DBT_BUILD', seconds, NULL)), 0) INTO :dbt_secs
    FROM OPS.PIPELINE_RUN_LOG WHERE graph_run_group_id = :gid;
  IF (n_failed > 0) THEN
    CALL OPS.SP_NOTIFY_PIPELINE('FAILED', :n_failed || ' of ' || :n_total || ' tasks failed:\n' || :failures
                                || '\nGold is NOT current - see OPS.V_PIPELINE_STATUS');
  ELSE
    CALL OPS.SP_NOTIFY_PIPELINE('COMPLETED', :n_total || ' tasks succeeded; dbt build ' || ROUND(:dbt_secs / 60, 1)
                                || ' min; QA passed; gold is current');
  END IF;

  -- Fail loudly: the run log is written first, then the finalizer itself fails, so task
  -- history, monitoring and anything polling T_FINALIZE see the failure too.
  IF (n_failed > 0) THEN
    RAISE graph_failed;
  END IF;
  RETURN 'graph ' || gid || ': ' || n_total || ' tasks, 0 failed';
END;
$$;

-- the business day processed by a run that starts at 01:10 Manila = yesterday (Manila)
CREATE OR REPLACE TASK OPS.T_ROOT
  WAREHOUSE = ETL_WH
  SCHEDULE = 'USING CRON 10 1 * * * Asia/Manila'
  SUSPEND_TASK_AFTER_NUM_FAILURES = 3
  USER_TASK_TIMEOUT_MS = 14400000
  CONFIG = '{"pipeline": "telco_daily"}'
  COMMENT = 'Daily telco pipeline: ingest -> snowpipe -> native pipelines -> dbt build -> QA'
AS CALL OPS.SP_NOTIFY_PIPELINE('STARTED', 'ingest -> snowpipe -> native health -> dbt build -> QA');

CREATE OR REPLACE TASK OPS.T_INGEST WAREHOUSE = ETL_WH
  COMMENT = 'Source systems: one business day of inserts / updates / deletes / late rows'
  AFTER OPS.T_ROOT
AS CALL INGEST.SP_INGEST_DAY(DATEADD('day', -1, CONVERT_TIMEZONE('Asia/Manila', CURRENT_TIMESTAMP())::DATE));

CREATE OR REPLACE TASK OPS.T_LOAD_FILES WAREHOUSE = ETL_WH
  COMMENT = 'Snowpipe: load the day''s CDR / XDR / payment files, reconcile rows'
  AFTER OPS.T_INGEST
AS CALL INGEST.SP_LOAD_FILES(DATEADD('day', -1, CONVERT_TIMEZONE('Asia/Manila', CURRENT_TIMESTAMP())::DATE));

CREATE OR REPLACE TASK OPS.T_NATIVE WAREHOUSE = ETL_WH
  COMMENT = 'Client''s native stored-procedure pipelines (PIPE / PIPE_X / MART)'
  AFTER OPS.T_LOAD_FILES
AS CALL OPS.SP_RUN_NATIVE(DATEADD('day', -1, CONVERT_TIMEZONE('Asia/Manila', CURRENT_TIMESTAMP())::DATE));

CREATE OR REPLACE TASK OPS.T_FRESHNESS WAREHOUSE = ETL_WH
  COMMENT = 'dbt source freshness (fails on error thresholds)'
  AFTER OPS.T_LOAD_FILES
AS EXECUTE DBT PROJECT OPS.TELCO_DBT ARGS = 'source freshness --target prod';

CREATE OR REPLACE TASK OPS.T_DBT_BUILD WAREHOUSE = ETL_WH
  COMMENT = 'dbt build: CDC silver, snapshots, incremental gold, tests, RAP re-attach, lineage publish'
  AFTER OPS.T_NATIVE, OPS.T_FRESHNESS
AS EXECUTE DBT PROJECT OPS.TELCO_DBT ARGS = 'build --target prod';

CREATE OR REPLACE TASK OPS.T_QA WAREHOUSE = ETL_WH
  COMMENT = 'Post-build controls (OPS.QA_RESULTS)'
  AFTER OPS.T_DBT_BUILD
AS CALL OPS.SP_QA_CHECKS(DATEADD('day', -1, CONVERT_TIMEZONE('Asia/Manila', CURRENT_TIMESTAMP())::DATE));

CREATE OR REPLACE TASK OPS.T_FINALIZE WAREHOUSE = ETL_WH FINALIZE = OPS.T_ROOT
  COMMENT = 'Finalizer: records every task of the graph run in OPS.PIPELINE_RUN_LOG'
AS CALL OPS.SP_FINALIZE();

-- children first, root last
ALTER TASK OPS.T_FINALIZE   RESUME;
ALTER TASK OPS.T_QA         RESUME;
ALTER TASK OPS.T_DBT_BUILD  RESUME;
ALTER TASK OPS.T_FRESHNESS  RESUME;
ALTER TASK OPS.T_NATIVE     RESUME;
ALTER TASK OPS.T_LOAD_FILES RESUME;
ALTER TASK OPS.T_INGEST     RESUME;
ALTER TASK OPS.T_ROOT       RESUME;
