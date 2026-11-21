-- =============================================================================
-- 07_native_qa.sql -- procedures the scheduler calls between ingestion and dbt / after dbt.
--   OPS.SP_RUN_NATIVE(d)  : the client's existing stored-procedure pipelines (PIPE / PIPE_X / MART)
--                           on their natural cadence; each step logged, failures don't block dbt
--   OPS.SP_QA_CHECKS(d)   : post-build controls written to OPS.QA_RESULTS; raises on FAIL
--   OPS.LEGACY_TASK_DECISIONS : what happened to each of the 14 pre-existing tasks
-- Owner: SYSADMIN (owner's rights over PIPE/PIPE_X/MART/GOLD); DBT_ROLE gets USAGE.
-- =============================================================================
USE ROLE SYSADMIN;
USE DATABASE TELCO_PROD_XL_V2;

CREATE TABLE IF NOT EXISTS OPS.NATIVE_RUN_LOG (
  BUSINESS_DATE DATE, STEP VARCHAR, STATUS VARCHAR, SECONDS NUMBER(10,1), RESULT VARCHAR,
  LOGGED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP());
CREATE TABLE IF NOT EXISTS OPS.QA_RESULTS (
  BUSINESS_DATE DATE, CHECK_NAME VARCHAR, STATUS VARCHAR, OBSERVED VARCHAR, EXPECTED VARCHAR,
  DETAIL VARCHAR, LOGGED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP());
GRANT SELECT ON TABLE OPS.NATIVE_RUN_LOG TO ROLE DBT_ROLE;
GRANT SELECT ON TABLE OPS.QA_RESULTS     TO ROLE DBT_ROLE;

-- SUPERSEDED by ops/sql/11_retire_pipe_external_tables.sql (PIPE / PIPE_X retired 2026-11-21).
-- Kept for history only; run 11 after this file.
CREATE OR REPLACE PROCEDURE OPS.SP_RUN_NATIVE_LEGACY(D DATE)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Runs the client''s native stored-procedure pipelines for business day D (replaces the suspended legacy tasks)'
EXECUTE AS OWNER
AS
$$
import time, datetime as dt

def run(session, d):
    if isinstance(d, str):
        d = dt.date.fromisoformat(d)
    month = d.replace(day=1)
    prev_month = (month - dt.timedelta(days=1)).replace(day=1)
    steps = []
    # daily
    steps += [("PIPE.SP_SCD2_MERGE", "call PIPE.SP_SCD2_MERGE()", "select system$stream_has_data('PIPE.STRM_SUBSCRIPTION')"),
              ("PIPE.SP_GOLD_CELL_DAILY_QUALITY", f"call PIPE.SP_GOLD_CELL_DAILY_QUALITY('{d}')", None),
              ("PIPE_X.SP_X_H16_SUBSCRIPTION_CDC_SCD1", "call PIPE_X.SP_X_H16_SUBSCRIPTION_CDC_SCD1(null)", "select system$stream_has_data('PIPE_X.STRM_X_SUBSCRIPTION')"),
              ("PIPE_X.SP_X_M26_STREAM_SMS_HOURLY", "call PIPE_X.SP_X_M26_STREAM_SMS_HOURLY()", "select system$stream_has_data('PIPE_X.STRM_X_CDR_SMS')"),
              ("PIPE_X.SP_X_M27_STREAM_DATA_SUBSCRIBER", "call PIPE_X.SP_X_M27_STREAM_DATA_SUBSCRIBER()", "select system$stream_has_data('PIPE_X.STRM_X_CDR_DATA')"),
              ("PIPE_X.SP_X_M28_STREAM_XDR_PARSE", "call PIPE_X.SP_X_M28_STREAM_XDR_PARSE()", "select system$stream_has_data('PIPE_X.STRM_X_XDR')"),
              ("MART.SP_REFRESH_DAILY_ARPU", f"call MART.SP_REFRESH_DAILY_ARPU('{month}')", None)]
    # month start: close the previous month
    if d.day == 1:
        steps += [(f"PIPE.{p}", f"call PIPE.{p}('{prev_month}')", None) for p in
                  ["SP_GOLD_SUBSCRIBER_MONTHLY_KPI", "SP_GOLD_REVENUE_RECON", "SP_GOLD_CUSTOMER_TIER",
                   "SP_GOLD_SUBSCRIBER_PROFILE"]]
        steps += [(f"PIPE.{p}", f"call PIPE.{p}('{d}')", None) for p in
                  ["SP_GOLD_PLAN", "SP_GOLD_PARTNER_RATE", "SP_GOLD_CHURN_RISK"]]
        steps += [("PIPE.SP_MONTH_END", f"call PIPE.SP_MONTH_END('{prev_month}')", None)]
    # billing close (H15 task graph: invoices -> payments -> recon) on the 2nd
    if d.day == 2:
        steps += [(f"PIPE_X.{p}", f"call PIPE_X.{p}(null)", None) for p in
                  ["SP_X_H15_STEP1_INVOICES", "SP_X_H15_STEP2_PAYMENTS", "SP_X_H15_STEP3_RECON"]]
    ok = fail = skipped = 0
    for name, stmt, cond in steps:
        t = time.time()
        try:
            if cond and str(session.sql(cond).collect()[0][0]).lower() not in ('true', '1'):
                status, res = 'SKIPPED', 'stream has no data'; skipped += 1
            else:
                r = session.sql(stmt).collect()
                status, res = 'SUCCEEDED', (str(r[0][0]) if r and len(r[0]) else '')[:500]; ok += 1
        except Exception as e:
            status, res = 'FAILED', str(e)[:500]; fail += 1
        res = res.replace("'", "''")
        session.sql(f"""insert into OPS.NATIVE_RUN_LOG (BUSINESS_DATE, STEP, STATUS, SECONDS, RESULT)
                        values ('{d}', '{name}', '{status}', {round(time.time() - t, 1)}, '{res}')""").collect()
    return f"native pipelines {d}: succeeded={ok} failed={fail} skipped={skipped}"
$$;

CREATE OR REPLACE PROCEDURE OPS.SP_QA_CHECKS(D DATE)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Post-build controls: arrival, load reconciliation, bronze->silver completeness, RLS coverage, native pipeline health'
EXECUTE AS OWNER
AS
$$
import datetime as dt

def run(session, d):
    if isinstance(d, str):
        d = dt.date.fromisoformat(d)
    one = lambda s: session.sql(s).collect()[0][0]
    res = []
    def add(name, status, obs, exp, detail=''):
        res.append((name, status, str(obs), str(exp), detail))

    # 1. data arrived for the business day (event dates in bronze)
    for tbl, col in [("USAGE.CDR_VOICE", "convert_timezone('Asia/Manila', call_start)::date"),
                     ("USAGE.CDR_DATA", "convert_timezone('UTC', 'Asia/Manila', session_start)::date"),
                     ("BILLING.DAILY_REVENUE", "business_date"),
                     ("NETWORK.CELL_PERFORMANCE_15MIN", "convert_timezone('UTC', 'Asia/Manila', interval_start)::date")]:
        mx = one(f"select max({col}) from {tbl} where {col} >= dateadd('day', -3, '{d}'::date)")
        add(f"arrival:{tbl}", 'PASS' if mx and mx >= d else 'FAIL', mx, f">= {d}")

    # 2. every exported file row reached bronze through Snowpipe
    r = session.sql(f"""select f.table_name, f.n, coalesce(l.n, 0) from
            (select table_name, sum(row_count) n from OPS.INGEST_RUN_LOG where business_date = '{d}' and operation = 'FILE_ROWS'
               and run_id = (select max_by(run_id, logged_at) from OPS.INGEST_RUN_LOG where business_date = '{d}' and operation = 'FILE_ROWS') group by 1) f
            left join (select table_name, max(row_count) n from OPS.INGEST_RUN_LOG where business_date = '{d}' and operation = 'SNOWPIPE_LOAD' group by 1) l
              on l.table_name = f.table_name""").collect()
    for t, exp, obs in r:
        add(f"snowpipe_reconciliation:{t}", 'PASS' if exp == obs else 'FAIL', obs, exp)

    # 3. bronze -> silver completeness for the day (distinct keys, same validity filters as silver)
    pairs = [("voice", "select count(distinct call_id) from USAGE.CDR_VOICE where duration_sec >= 0 and subscription_id is not null and convert_timezone('UTC','Asia/Manila', convert_timezone('UTC', call_start)::timestamp_ntz)::date = '{d}'",
                       "select count(*) from DBT_TELCO_SILVER.SLV_CDR_VOICE where event_date = '{d}'"),
             ("data",  "select count(distinct session_id) from USAGE.CDR_DATA where subscription_id is not null and convert_timezone('UTC','Asia/Manila', session_start)::date = '{d}'",
                       "select count(*) from DBT_TELCO_SILVER.SLV_CDR_DATA where event_date = '{d}'"),
             ("revenue", "select round(sum(revenue_php), 2) from BILLING.DAILY_REVENUE where business_date = '{d}'",
                         "select round(sum(revenue_php), 2) from DBT_TELCO_SILVER.SLV_DAILY_REVENUE where business_date = '{d}'")]
    for name, b, s in pairs:
        bv, sv = one(b.format(d=d)), one(s.format(d=d))
        add(f"bronze_vs_silver:{name}", 'PASS' if bv == sv else 'FAIL', sv, bv)

    # 4. every region is mapped to an analyst role, and RAP_REGION is on every gold table with REGION_ID
    unmapped = one("""select count(*) from REF.GEO_REGION g where not exists (select 1 from GOV.REGION_ACCESS_MAP m
                      where m.region_name = coalesce(nullif(trim(g.region_name), ''), 'REGION_' || g.region_id))""")
    add("rls:unmapped_regions", 'PASS' if unmapped == 0 else 'FAIL', unmapped, 0)
    tables = [r[0] for r in session.sql("""select distinct c.table_name from information_schema.columns c
                 join information_schema.tables t on t.table_schema = c.table_schema and t.table_name = c.table_name
                where c.table_schema = 'DBT_TELCO_GOLD' and c.column_name in ('REGION_ID', 'HOME_REGION_ID', 'REGION_NAME')
                  and t.table_type = 'BASE TABLE'""").collect()]
    missing = []
    for t in tables:
        n = one(f"""select count(*) from table(information_schema.policy_references(
                      ref_entity_name => 'TELCO_PROD_XL_V2.DBT_TELCO_GOLD.{t}', ref_entity_domain => 'table'))
                    where policy_name in ('RAP_REGION', 'RAP_REGION_NAME')""")
        if n == 0: missing.append(t)
    add("rls:gold_tables_without_rap", 'PASS' if not missing else 'FAIL', len(missing), 0, ','.join(missing)[:500])
    add("rls:regional_gold_tables", 'INFO', len(tables), '', '')

    # 5. native pipelines of the same graph run
    # NATIVE_PIPE (N01-N40) health recorded by OPS.SP_RUN_NATIVE: warn only, it runs on its own schedule
    nf = session.sql(f"""select count(*), listagg(step, ',') from OPS.NATIVE_RUN_LOG where business_date = '{d}'
                         and status in ('FAILED', 'PARTIAL') and logged_at > dateadd('hour', -12, current_timestamp())""").collect()[0]
    add("native_pipelines:failed_or_partial", 'WARN' if nf[0] else 'PASS', nf[0], 0, (nf[1] or '')[:400])

    # 6. external tables: the day's vendor + partner files are visible in the S3 external tables
    for ext in ('INGEST.EXT_DEVICE_FEED', 'INGEST.EXT_PARTNER_RATE'):
        n = one(f"select count(*) from {ext} where feed_date = '{d}'")
        add(f"external_table:{ext}", 'PASS' if n and n > 0 else 'FAIL', n, '> 0')

    for name, status, obs, exp, det in res:
        det = det.replace("'", "''")
        session.sql(f"""insert into OPS.QA_RESULTS (BUSINESS_DATE, CHECK_NAME, STATUS, OBSERVED, EXPECTED, DETAIL)
                        values ('{d}', '{name}', '{status}', '{obs}', '{exp}', '{det}')""").collect()
    fails = [r[0] for r in res if r[1] == 'FAIL']
    if fails:
        raise Exception(f"QA FAILED for {d}: " + ", ".join(fails))
    return f"QA {d}: {len(res)} checks, 0 failed"
$$;

-- (SP_RUN_NATIVE is defined in 11_retire_pipe_external_tables.sql)
GRANT USAGE ON PROCEDURE OPS.SP_QA_CHECKS(DATE)  TO ROLE DBT_ROLE;
-- SYSADMIN reads silver/gold through the role hierarchy (DBT_ROLE is granted to SYSADMIN).

-- ---------------------------------------------------------------- legacy tasks
CREATE OR REPLACE TABLE OPS.LEGACY_TASK_DECISIONS (
  TASK_NAME VARCHAR, STATE_BEFORE VARCHAR, DECISION VARCHAR, NOW_RUN_BY VARCHAR, REASON VARCHAR);
INSERT INTO OPS.LEGACY_TASK_DECISIONS VALUES
 ('MART.TASK_NIGHTLY_ROOT',        'suspended', 'RETIRED',  'OPS.T_ROOT',                         'replaced by the single 01:10 Manila graph'),
 ('MART.TASK_REFRESH_ARPU',        'suspended', 'RETIRED',  'OPS.SP_RUN_NATIVE (daily)',          'proc kept; date no longer hard-coded to 2026-09-01'),
 ('MART.TASK_CAPTURE_SUB_CHANGES', 'suspended', 'RETIRED',  'dbt CDC on CORE.SUBSCRIPTION + snapshots', 'stream log superseded by change tracking + snap_subscription'),
 ('PIPE.TASK_ROOT',                'suspended', 'RETIRED',  'OPS.T_ROOT',                         'graph root replaced'),
 ('PIPE.TASK_SCD2',                'started (orphaned, root suspended)', 'RETIRED', 'OPS.SP_RUN_NATIVE (daily)', 'PIPE.SP_SCD2_MERGE still runs, from the new graph'),
 ('PIPE.TASK_AUDIT',               'started (orphaned, root suspended)', 'RETIRED', 'OPS.INGEST_RUN_LOG + OPS.QA_RESULTS', 'audit superseded by ingest/load reconciliation'),
 ('PIPE.TASK_FINALIZE',            'started (orphaned, root suspended)', 'RETIRED', 'OPS.T_FINALIZE',                     'finalizer replaced'),
 ('PIPE_X.TASK_X_H15_ROOT',        'suspended', 'RETIRED',  'OPS.SP_RUN_NATIVE (2nd of month)',   'billing-close chain runs inside the new graph'),
 ('PIPE_X.TASK_X_H15_PAYMENTS',    'suspended', 'RETIRED',  'OPS.SP_RUN_NATIVE (2nd of month)',   'step 2 of the chain'),
 ('PIPE_X.TASK_X_H15_RECON',       'suspended', 'RETIRED',  'OPS.SP_RUN_NATIVE (2nd of month)',   'step 3 of the chain'),
 ('PIPE_X.TASK_X_H16',             'suspended', 'RETIRED',  'OPS.SP_RUN_NATIVE (daily)',          'was every 30 min; daily is the business SLA'),
 ('PIPE_X.TASK_X_M26',             'suspended', 'RETIRED',  'OPS.SP_RUN_NATIVE (daily)',          'stream-driven, runs when the stream has data'),
 ('PIPE_X.TASK_X_M27',             'suspended', 'RETIRED',  'OPS.SP_RUN_NATIVE (daily)',          'stream-driven, runs when the stream has data'),
 ('PIPE_X.TASK_X_M28',             'suspended', 'RETIRED',  'OPS.SP_RUN_NATIVE (daily)',          'stream-driven, runs when the stream has data');
GRANT SELECT ON TABLE OPS.LEGACY_TASK_DECISIONS TO ROLE DBT_ROLE;

-- The three orphaned children are 'started' but can never fire (their root is suspended);
-- suspend them so the account's task list reflects reality. Roots stay suspended.
ALTER TASK PIPE.TASK_AUDIT    SUSPEND;
ALTER TASK PIPE.TASK_SCD2     SUSPEND;
ALTER TASK PIPE.TASK_FINALIZE SUSPEND;
