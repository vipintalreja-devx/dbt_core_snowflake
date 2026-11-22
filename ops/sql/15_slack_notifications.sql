-- =============================================================================
-- 15_slack_notifications.sql -- Slack messages for the telco_daily graph: STARTED, COMPLETED, FAILED.
-- Event-driven (no polling alert, no hourly messages):
--   T_ROOT     -> OPS.SP_NOTIFY_PIPELINE('STARTED')
--   T_FINALIZE -> OPS.SP_FINALIZE() posts COMPLETED or FAILED (with failing tasks), then raises on failure
-- The webhook secret is the part of the Slack URL after https://hooks.slack.com/services/
-- and lives only in the Snowflake SECRET below -- never in this repo.
-- =============================================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE TELCO_PROD_XL_V2;

-- 1. secret: replace <T…/B…/…> with the path part of the channel's incoming-webhook URL
CREATE SECRET IF NOT EXISTS OPS.SLACK_TELCO_WEBHOOK
  TYPE = GENERIC_STRING
  SECRET_STRING = '<T…/B…/…>'
  COMMENT = 'Slack incoming webhook (telco pipeline channel)';

-- 2. notification integration (Slack expects {"text": ...})
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS TELCO_SLACK
  TYPE = WEBHOOK
  ENABLED = TRUE
  WEBHOOK_URL = 'https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
  WEBHOOK_SECRET = TELCO_PROD_XL_V2.OPS.SLACK_TELCO_WEBHOOK
  WEBHOOK_BODY_TEMPLATE = '{"text": "SNOWFLAKE_WEBHOOK_MESSAGE"}'
  WEBHOOK_HEADERS = ('Content-Type' = 'application/json')
  COMMENT = 'telco_daily pipeline notifications to Slack';

GRANT USAGE ON INTEGRATION TELCO_SLACK TO ROLE DBT_ROLE;
GRANT USAGE ON SECRET OPS.SLACK_TELCO_WEBHOOK TO ROLE DBT_ROLE;
GRANT READ ON SECRET OPS.SLACK_TELCO_WEBHOOK TO ROLE DBT_ROLE;

-- 3. notifier (owner DBT_ROLE = task owner)
USE ROLE DBT_ROLE;
CREATE OR REPLACE PROCEDURE OPS.SP_NOTIFY_PIPELINE(EVENT VARCHAR, DETAIL VARCHAR DEFAULT NULL)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Posts a telco_daily pipeline event (STARTED / COMPLETED / FAILED) to Slack; never fails the caller'
EXECUTE AS CALLER
AS
$$
DECLARE
  bdate DATE DEFAULT DATEADD('day', -1, CONVERT_TIMEZONE('Asia/Manila', CURRENT_TIMESTAMP())::DATE);
  icon VARCHAR;
  msg VARCHAR;
BEGIN
  icon := CASE :EVENT WHEN 'STARTED' THEN ':arrow_forward:' WHEN 'COMPLETED' THEN ':white_check_mark:' ELSE ':x:' END;
  msg := icon || ' *telco_daily ' || :EVENT || '* - business day ' || TO_VARCHAR(bdate)
         || ' - ' || TO_VARCHAR(CONVERT_TIMEZONE('Asia/Manila', CURRENT_TIMESTAMP()), 'YYYY-MM-DD HH24:MI') || ' Manila'
         || IFF(:DETAIL IS NULL, '', '\n' || :DETAIL);
  -- JSON-safe: the integration drops the message into a JSON string
  msg := REPLACE(REPLACE(REPLACE(msg, '\\', '\\\\'), '"', '\''), '\n', '\\n');
  CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
         SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(:msg),
         SNOWFLAKE.NOTIFICATION.INTEGRATION('TELCO_SLACK'));
  RETURN 'notified: ' || :EVENT;
EXCEPTION
  WHEN OTHER THEN
    RETURN 'notification failed (pipeline unaffected): ' || SQLERRM;
END;
$$;
