-- =============================================================================
-- 14_pipeline_status.sql -- one place that says whether gold is current.
--   OPS.V_PIPELINE_STATUS : latest graph attempt, last clean run, IS_GOLD_CURRENT
--   read by the BI refresh procedures (BI_SIM) before they query gold.
-- "Current" = the latest graph run succeeded AND processed yesterday's Manila business day.
-- =============================================================================
USE ROLE DBT_ROLE;
USE DATABASE TELCO_PROD_XL_V2;
CREATE OR REPLACE VIEW OPS.V_PIPELINE_STATUS
  COMMENT = 'Is gold current? Latest telco_daily graph attempt vs last clean run (from OPS.PIPELINE_RUN_LOG)'
AS
WITH g AS (
    SELECT graph_run_group_id, business_date, state, return_value, logged_at
      FROM OPS.PIPELINE_RUN_LOG
     WHERE task_name = 'GRAPH'
), latest AS (
    SELECT * FROM g QUALIFY ROW_NUMBER() OVER (ORDER BY logged_at DESC) = 1
), last_ok AS (
    SELECT * FROM g WHERE state = 'SUCCEEDED' QUALIFY ROW_NUMBER() OVER (ORDER BY logged_at DESC) = 1
), failed_tasks AS (
    SELECT LISTAGG(task_name || ': ' || LEFT(COALESCE(error_message, state), 150), ' | ') AS failures
      FROM OPS.PIPELINE_RUN_LOG
     WHERE graph_run_group_id = (SELECT graph_run_group_id FROM latest)
       AND task_name <> 'GRAPH' AND state IN ('FAILED', 'CANCELLED')
)
SELECT l.state                                   AS latest_run_state,
       l.business_date                           AS latest_run_business_date,
       l.logged_at                               AS latest_run_finished_at,
       l.return_value                            AS latest_run_summary,
       f.failures                                AS latest_run_failures,
       o.business_date                           AS last_clean_business_date,
       o.logged_at                               AS last_clean_finished_at,
       DATEDIFF('hour', o.logged_at, CURRENT_TIMESTAMP()) AS hours_since_last_clean_run,
       (l.state = 'SUCCEEDED'
        AND l.business_date >= DATEADD('day', -1, CONVERT_TIMEZONE('Asia/Manila', CURRENT_TIMESTAMP())::DATE)) AS is_gold_current
  FROM latest l
  LEFT JOIN last_ok o ON TRUE
  LEFT JOIN failed_tasks f ON TRUE;

USE ROLE SYSADMIN;
GRANT USAGE ON SCHEMA OPS TO ROLE TELCO_ANALYST_NCR;
GRANT USAGE ON SCHEMA OPS TO ROLE TELCO_ANALYST_LUZON;
GRANT USAGE ON SCHEMA OPS TO ROLE TELCO_ANALYST_VISMIN;
USE ROLE DBT_ROLE;
GRANT SELECT ON VIEW OPS.V_PIPELINE_STATUS TO ROLE TELCO_ANALYST_NCR;
GRANT SELECT ON VIEW OPS.V_PIPELINE_STATUS TO ROLE TELCO_ANALYST_LUZON;
GRANT SELECT ON VIEW OPS.V_PIPELINE_STATUS TO ROLE TELCO_ANALYST_VISMIN;
