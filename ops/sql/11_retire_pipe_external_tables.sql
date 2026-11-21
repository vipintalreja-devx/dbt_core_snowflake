-- =============================================================================
-- 11_retire_pipe_external_tables.sql
--   A. Retire PIPE / PIPE_X / MART procedures from the nightly graph. The client's native
--      pipelines now live in NATIVE_PIPE (N01-N40, own root task 03:15 Manila); OPS.T_NATIVE
--      only records their health for QA. Nothing in PIPE / PIPE_X is dropped.
--   B. Two external tables on the S3 landing bucket (integration S3_TELCO_NATIVE), prefix ingest/:
--        INGEST.EXT_DEVICE_FEED   vendor device + price updates   (JSON, daily)
--        INGEST.EXT_PARTNER_RATE  MVNO partner rate-card files     (CSV, daily)
--      Written by the ingestor, refreshed by AUTO_REFRESH (bucket SQS notifications) and,
--      deterministically, by INGEST.SP_LOAD_FILES after the Snowpipe step.
-- =============================================================================
USE ROLE SYSADMIN;
USE DATABASE TELCO_PROD_XL_V2;

-- ---------------------------------------------------------------- B. external tables
CREATE FILE FORMAT IF NOT EXISTS INGEST.FF_CSV_GZ_HDR
  TYPE = CSV COMPRESSION = GZIP SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('\\N', '');
CREATE STAGE IF NOT EXISTS INGEST.S3_FEEDS
  URL = 's3://devx-telco-native-landing/ingest/'
  STORAGE_INTEGRATION = S3_TELCO_NATIVE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Vendor + partner files (external tables). Shares the native landing bucket, own prefix ingest/';

CREATE EXTERNAL TABLE IF NOT EXISTS INGEST.EXT_DEVICE_FEED (
    FEED_DATE   DATE      AS (TRY_TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1))),
    DEVICE_ID   NUMBER    AS (VALUE:device_id::NUMBER),
    MAKER       VARCHAR   AS (VALUE:maker::VARCHAR),
    PRICE_PHP   NUMBER(12,2) AS (VALUE:price_php::NUMBER(12,2)),
    RAW         VARIANT   AS (VALUE),
    FILE_NAME   VARCHAR   AS (METADATA$FILENAME)
  )
  PARTITION BY (FEED_DATE)
  LOCATION = @INGEST.S3_FEEDS/vendor_devices/
  FILE_FORMAT = (FORMAT_NAME = 'INGEST.FF_JSON_GZ')
  AUTO_REFRESH = TRUE
  COMMENT = 'Daily vendor device catalogue / price update files (s3://devx-telco-native-landing/ingest/vendor_devices/dt=YYYY-MM-DD/)';

CREATE EXTERNAL TABLE IF NOT EXISTS INGEST.EXT_PARTNER_RATE (
    FEED_DATE        DATE          AS (TRY_TO_DATE(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1))),
    PARTNER_BRAND_ID NUMBER        AS (VALUE:c1::NUMBER),
    SERVICE_TYPE     VARCHAR       AS (VALUE:c2::VARCHAR),
    UNIT             VARCHAR       AS (VALUE:c3::VARCHAR),
    RATE_PHP         NUMBER(12,6)  AS (VALUE:c4::NUMBER(12,6)),
    EFFECTIVE_FROM   DATE          AS (VALUE:c5::DATE),
    EFFECTIVE_TO     DATE          AS (VALUE:c6::DATE),
    FILE_NAME        VARCHAR       AS (METADATA$FILENAME)
  )
  PARTITION BY (FEED_DATE)
  LOCATION = @INGEST.S3_FEEDS/partner_rates/
  FILE_FORMAT = (FORMAT_NAME = 'INGEST.FF_CSV_GZ_HDR')
  AUTO_REFRESH = TRUE
  COMMENT = 'Daily MVNO partner wholesale rate files (s3://devx-telco-native-landing/ingest/partner_rates/dt=YYYY-MM-DD/)';

GRANT SELECT ON EXTERNAL TABLE INGEST.EXT_DEVICE_FEED  TO ROLE DBT_ROLE;
GRANT SELECT ON EXTERNAL TABLE INGEST.EXT_PARTNER_RATE TO ROLE DBT_ROLE;

-- Vendor / partner file writer (called by SP_INGEST_DAY; deterministic per day, SINGLE file
-- with a fixed name, so a re-run overwrites instead of duplicating).
CREATE OR REPLACE PROCEDURE INGEST.SP_WRITE_VENDOR_FILES(D DATE)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Writes the day''s vendor device file and partner rate file to s3://.../ingest/'
EXECUTE AS OWNER
AS
$$
DECLARE
  ds VARCHAR DEFAULT TO_VARCHAR(:D, 'YYYY-MM-DD');
  n_dev INTEGER DEFAULT 0;
  n_rate INTEGER DEFAULT 0;
  stmt VARCHAR;
  rs RESULTSET;
BEGIN
  stmt := 'COPY INTO @INGEST.S3_FEEDS/vendor_devices/dt=' || ds || '/devices_' || ds || '.json.gz FROM (
             SELECT OBJECT_INSERT(raw, ''price_php'',
                      ROUND(raw:price_php::NUMBER(12,2) * (1 + (MOD(ABS(HASH(raw:device_id, ''' || ds || ''')), 11) - 5) / 100), 2), TRUE)
               FROM LANDING.DEVICE_FEED_RAW
              WHERE MOD(ABS(HASH(raw:device_id, ''' || ds || ''')), 100) = 0)
           FILE_FORMAT = (FORMAT_NAME = ''INGEST.FF_JSON_GZ'') SINGLE = TRUE OVERWRITE = TRUE MAX_FILE_SIZE = 50000000';
  rs := (EXECUTE IMMEDIATE :stmt);
  FOR r IN rs DO n_dev := r."rows_unloaded"; END FOR;

  stmt := 'COPY INTO @INGEST.S3_FEEDS/partner_rates/dt=' || ds || '/rates_' || ds || '.csv.gz FROM (
             SELECT partner_brand_id, service_type, unit,
                    ROUND(rate_php * (1 + (MOD(ABS(HASH(partner_brand_id, service_type, ''' || ds || ''')), 7) - 3) / 100), 6),
                    DATEADD(''day'', 1, ''' || ds || '''::DATE), LAST_DAY(DATEADD(''day'', 1, ''' || ds || '''::DATE))
               FROM PARTNER.PARTNER_RATE_CARD
              ORDER BY HASH(partner_brand_id, service_type, unit, effective_from, ''' || ds || ''') LIMIT 120)
           FILE_FORMAT = (TYPE = CSV COMPRESSION = GZIP FIELD_OPTIONALLY_ENCLOSED_BY = ''"'')
           HEADER = TRUE SINGLE = TRUE OVERWRITE = TRUE';
  rs := (EXECUTE IMMEDIATE :stmt);
  FOR r IN rs DO n_rate := r."rows_unloaded"; END FOR;
  RETURN 'devices=' || n_dev || ', partner_rates=' || n_rate;
END;
$$;

-- ---------------------------------------------------------------- A. retire PIPE / PIPE_X
-- T_NATIVE now records the health of the NATIVE_PIPE pipelines (own schedule, 03:15 Manila):
-- last 24 h of NATIVE_PIPE.N_RUN_LOG + failed NATIVE_PIPE tasks -> OPS.NATIVE_RUN_LOG.
CREATE OR REPLACE PROCEDURE OPS.SP_RUN_NATIVE(D DATE)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Native pipeline health: NATIVE_PIPE (N01-N40) run log + task failures of the last 24 h. PIPE / PIPE_X are retired and no longer called.'
EXECUTE AS OWNER
AS
$$
DECLARE
  n_ok INTEGER; n_bad INTEGER; n_task_fail INTEGER;
BEGIN
  INSERT INTO OPS.NATIVE_RUN_LOG (BUSINESS_DATE, STEP, STATUS, SECONDS, RESULT)
  SELECT :D, 'NATIVE_PIPE.' || pipeline_id || '.' || step,
         IFF(status = 'OK', 'SUCCEEDED', status),
         ROUND(DATEDIFF('millisecond', started_at, finished_at) / 1000, 1),
         LEFT('run_date=' || run_date || ' rows=' || COALESCE(rows_written, 0) || ' ' || COALESCE(message, ''), 500)
    FROM NATIVE_PIPE.N_RUN_LOG
   WHERE started_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
  QUALIFY ROW_NUMBER() OVER (PARTITION BY pipeline_id, step ORDER BY started_at DESC) = 1;

  INSERT INTO OPS.NATIVE_RUN_LOG (BUSINESS_DATE, STEP, STATUS, SECONDS, RESULT)
  SELECT :D, 'NATIVE_PIPE.task.' || name, 'FAILED', NULL,
         LEFT(COUNT(*) || ' failed run(s) in 24 h; last: ' || MAX_BY(error_message, scheduled_time), 500)
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()), RESULT_LIMIT => 10000))
   WHERE schema_name = 'NATIVE_PIPE' AND state = 'FAILED'
   GROUP BY name;

  SELECT COUNT_IF(status = 'SUCCEEDED'), COUNT_IF(status <> 'SUCCEEDED' AND step NOT LIKE 'NATIVE_PIPE.task.%'),
         COUNT_IF(step LIKE 'NATIVE_PIPE.task.%')
    INTO :n_ok, :n_bad, :n_task_fail
    FROM OPS.NATIVE_RUN_LOG WHERE business_date = :D AND logged_at >= DATEADD('minute', -5, CURRENT_TIMESTAMP());
  RETURN 'NATIVE_PIPE health ' || D || ': ok steps=' || n_ok || ', failed/partial steps=' || n_bad || ', failing tasks=' || n_task_fail;
END;
$$;
GRANT USAGE ON PROCEDURE OPS.SP_RUN_NATIVE(DATE) TO ROLE DBT_ROLE;

UPDATE OPS.LEGACY_TASK_DECISIONS
   SET NOW_RUN_BY = 'nothing (PIPE / PIPE_X retired 2026-11-21)',
       REASON = REASON || '; retired with PIPE/PIPE_X - replaced by the dbt medallion and NATIVE_PIPE N01-N40'
 WHERE NOW_RUN_BY LIKE 'OPS.SP_RUN_NATIVE%' AND REASON NOT LIKE '%retired with PIPE%';
INSERT INTO OPS.LEGACY_TASK_DECISIONS
SELECT * FROM VALUES
 ('PIPE (16 procedures)',   'not scheduled', 'RETIRED', 'dbt medallion (DBT_TELCO_*)',  'retired 2026-11-21: outputs re-implemented in dbt; nothing calls PIPE any more; objects kept, not dropped'),
 ('PIPE_X (90 procedures)', 'not scheduled', 'RETIRED', 'dbt agg_/anl_/adv_ models + NATIVE_PIPE', 'retired 2026-11-21: ported to dbt; new native work lives in NATIVE_PIPE N01-N40; objects kept, not dropped'),
 ('MART.SP_REFRESH_DAILY_ARPU', 'not scheduled', 'RETIRED', 'dbt mart_arpu_monthly', 'retired 2026-11-21 with PIPE / PIPE_X')
 AS v(a, b, c, d, e)
WHERE NOT EXISTS (SELECT 1 FROM OPS.LEGACY_TASK_DECISIONS WHERE TASK_NAME = v.a);

-- task comment reflects the new role (task body unchanged: CALL OPS.SP_RUN_NATIVE)
USE ROLE DBT_ROLE;
ALTER TASK OPS.T_ROOT SUSPEND;
ALTER TASK OPS.T_NATIVE SET COMMENT = 'Native pipeline health: NATIVE_PIPE (N01-N40) run log + failed tasks -> OPS.NATIVE_RUN_LOG (PIPE / PIPE_X retired)';
ALTER TASK OPS.T_ROOT RESUME;
