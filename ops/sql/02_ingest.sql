-- =============================================================================
-- 02_ingest.sql -- Batch 2: simulated source systems.
--   * internal stage + file formats + 5 Snowpipes (CDR voice/data/SMS, XDR, payment)
--   * control tables (feed watermarks, replay maps, OPS.INGEST_RUN_LOG)
--   * INGEST.SP_INGEST_DAY(d)  : one business day of inserts / updates / deletes / late rows
--   * INGEST.SP_LOAD_FILES(d)  : triggers the pipes and waits until the day's files are loaded
-- Owner: SYSADMIN (same owner as the source tables). Idempotent per business day.
-- Source-table shapes are NOT changed.
-- =============================================================================
USE ROLE SYSADMIN;
USE DATABASE TELCO_PROD_XL_V2;
USE SCHEMA INGEST;

-- ---------------------------------------------------------------- files
CREATE FILE FORMAT IF NOT EXISTS INGEST.FF_CSV_GZ
  TYPE = CSV COMPRESSION = GZIP FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('\\N') EMPTY_FIELD_AS_NULL = FALSE ESCAPE_UNENCLOSED_FIELD = NONE;
CREATE FILE FORMAT IF NOT EXISTS INGEST.FF_JSON_GZ TYPE = JSON COMPRESSION = GZIP;
CREATE STAGE IF NOT EXISTS INGEST.STG_FEEDS
  COMMENT = 'Landing zone for mediation (CDR/XDR) and payment-gateway files';

-- ---------------------------------------------------------------- control
CREATE TABLE IF NOT EXISTS INGEST.FEED_WATERMARK (
  FEED VARCHAR PRIMARY KEY, LAST_BUSINESS_DATE DATE, UPDATED_AT TIMESTAMP_LTZ);
-- Initialise each feed at the last date already present in the source table, so the
-- replay never duplicates the September bill-cycle tail that already exists.
MERGE INTO INGEST.FEED_WATERMARK t USING (
  SELECT 'DAILY'      AS FEED, '2026-09-30'::DATE AS D UNION ALL   -- CDR/XDR/revenue/network/care/marketing
  SELECT 'PAYMENT',            '2026-10-13'::DATE        UNION ALL
  SELECT 'ADJUSTMENT',         '2026-10-19'::DATE        UNION ALL
  SELECT 'DUNNING',            '2026-11-04'::DATE        UNION ALL
  SELECT 'BILL_CYCLE',         '2026-09-30'::DATE
) s ON t.FEED = s.FEED
WHEN NOT MATCHED THEN INSERT (FEED, LAST_BUSINESS_DATE, UPDATED_AT) VALUES (s.FEED, s.D, CURRENT_TIMESTAMP());

CREATE TABLE IF NOT EXISTS INGEST.REPLAY_MAP (
  ENTITY VARCHAR, SRC_ID NUMBER, NEW_ID NUMBER, SHIFT_DAYS NUMBER, BUSINESS_DATE DATE);
CREATE TRANSIENT TABLE IF NOT EXISTS INGEST.WRK_MOVES (
  SUBSCRIPTION_ID NUMBER, STATUS VARCHAR, NEW_STATUS VARCHAR);
CREATE TABLE IF NOT EXISTS INGEST.INVOICE_MAP (
  SRC_INVOICE_ID VARCHAR, NEW_INVOICE_ID VARCHAR, BILLING_MONTH DATE);

CREATE TABLE IF NOT EXISTS OPS.INGEST_RUN_LOG (
  RUN_ID VARCHAR, BUSINESS_DATE DATE, TABLE_NAME VARCHAR, OPERATION VARCHAR,
  ROW_COUNT NUMBER, NOTE VARCHAR, LOGGED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP());
GRANT SELECT ON TABLE OPS.INGEST_RUN_LOG TO ROLE DBT_ROLE;

-- ---------------------------------------------------------------- Snowpipes
-- Internal stage => no event notifications; the scheduler calls ALTER PIPE ... REFRESH.
CREATE PIPE IF NOT EXISTS INGEST.PIPE_CDR_VOICE COMMENT = 'Mediation voice CDR files' AS
COPY INTO USAGE.CDR_VOICE (CALL_ID, SUBSCRIPTION_ID, CALLING_MSISDN, CALLED_MSISDN, CELL_ID, CALL_START,
                           DURATION_SEC, IS_ROAMING, ROAMING_COUNTRY, CHARGE_PHP, CALL_TYPE, INGESTED_AT)
FROM (SELECT $1, $2, $3, $4, $5, TO_TIMESTAMP_TZ($6, 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZH:TZM'),
             $7, $8, $9, $10, $11, METADATA$START_SCAN_TIME
        FROM @INGEST.STG_FEEDS/cdr_voice/)
FILE_FORMAT = (FORMAT_NAME = 'INGEST.FF_CSV_GZ');

CREATE PIPE IF NOT EXISTS INGEST.PIPE_CDR_DATA COMMENT = 'Mediation data-session CDR files' AS
COPY INTO USAGE.CDR_DATA (SESSION_ID, SUBSCRIPTION_ID, CELL_ID, SESSION_START, SESSION_END, BYTES_UP,
                          BYTES_DOWN, CUMULATIVE_BYTES, APN, RAT, IS_ROAMING, INGESTED_AT)
FROM (SELECT $1, $2, $3, TO_TIMESTAMP_NTZ($4, 'YYYY-MM-DD"T"HH24:MI:SS.FF3'), TO_TIMESTAMP_NTZ($5, 'YYYY-MM-DD"T"HH24:MI:SS.FF3'),
             $6, $7, $8, $9, $10, $11, METADATA$START_SCAN_TIME
        FROM @INGEST.STG_FEEDS/cdr_data/)
FILE_FORMAT = (FORMAT_NAME = 'INGEST.FF_CSV_GZ');

CREATE PIPE IF NOT EXISTS INGEST.PIPE_CDR_SMS COMMENT = 'Mediation SMS CDR files' AS
COPY INTO USAGE.CDR_SMS (SMS_ID, SUBSCRIPTION_ID, SENDER_MSISDN, RECIPIENT_MSISDN, CELL_ID, SENT_AT,
                         IS_ROAMING, SMS_TYPE, CHARGE_PHP, INGESTED_AT)
FROM (SELECT $1, $2, $3, $4, $5, TO_TIMESTAMP_TZ($6, 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZH:TZM'),
             $7, $8, $9, METADATA$START_SCAN_TIME
        FROM @INGEST.STG_FEEDS/cdr_sms/)
FILE_FORMAT = (FORMAT_NAME = 'INGEST.FF_CSV_GZ');

CREATE PIPE IF NOT EXISTS INGEST.PIPE_XDR COMMENT = 'Mediation xDR JSON files' AS
COPY INTO USAGE.XDR_MEDIATION_RAW (RECORD_ID, SOURCE_SYSTEM, FILE_NAME, RECEIVED_AT, PAYLOAD, PARSE_STATUS)
FROM (SELECT $1:RECORD_ID::NUMBER, $1:SOURCE_SYSTEM::VARCHAR, SPLIT_PART(METADATA$FILENAME, '/', -1),
             $1:RECEIVED_AT::TIMESTAMP_LTZ, $1:PAYLOAD, $1:PARSE_STATUS::VARCHAR
        FROM @INGEST.STG_FEEDS/xdr/)
FILE_FORMAT = (FORMAT_NAME = 'INGEST.FF_JSON_GZ');

CREATE PIPE IF NOT EXISTS INGEST.PIPE_PAYMENT COMMENT = 'Payment-gateway settlement files' AS
COPY INTO BILLING.PAYMENT (PAYMENT_ID, ACCOUNT_ID, INVOICE_ID, PAID_AT, AMOUNT, PAYMENT_METHOD)
FROM (SELECT $1, $2, $3, TO_TIMESTAMP_LTZ($4, 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZH:TZM'), $5, $6
        FROM @INGEST.STG_FEEDS/payment/)
FILE_FORMAT = (FORMAT_NAME = 'INGEST.FF_CSV_GZ');

GRANT MONITOR ON PIPE INGEST.PIPE_CDR_VOICE TO ROLE DBT_ROLE;
GRANT MONITOR ON PIPE INGEST.PIPE_CDR_DATA  TO ROLE DBT_ROLE;
GRANT MONITOR ON PIPE INGEST.PIPE_CDR_SMS   TO ROLE DBT_ROLE;
GRANT MONITOR ON PIPE INGEST.PIPE_XDR       TO ROLE DBT_ROLE;
GRANT MONITOR ON PIPE INGEST.PIPE_PAYMENT   TO ROLE DBT_ROLE;

-- ---------------------------------------------------------------- ingestor
CREATE OR REPLACE PROCEDURE INGEST.SP_INGEST_DAY(D DATE)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Simulates one business day of source-system activity (inserts, updates, deletes, late + duplicate rows)'
EXECUTE AS OWNER
AS
$$
import datetime as dt, uuid

def la_off(x):
    # America/Los_Angeles: DST from the 2nd Sunday of March to the 1st Sunday of November
    mar = dt.date(x.year, 3, 8);  mar += dt.timedelta(days=(6 - mar.weekday()) % 7)
    nov = dt.date(x.year, 11, 1); nov += dt.timedelta(days=(6 - nov.weekday()) % 7)
    return '-07:00' if mar <= x < nov else '-08:00'

def run(session, d):
    if isinstance(d, str):
        d = dt.date.fromisoformat(d)
    session.sql("begin transaction").collect()
    try:
        res = _run(session, d)
        session.sql("commit").collect()
        return res
    except Exception:
        session.sql("rollback").collect()
        raise

def _run(session, d):
    run_id = 'ING-' + d.isoformat() + '-' + uuid.uuid4().hex[:6]
    sql = lambda s: session.sql(s).collect()
    def n(rows):
        if not rows: return 0
        r = rows[0].as_dict()
        for k in ('number of rows inserted', 'number of rows updated', 'number of rows deleted', 'rows_unloaded'):
            if k in r: return int(r[k])
        return int(list(r.values())[0] or 0)
    def log(tbl, op, cnt, note=''):
        sql(f"insert into OPS.INGEST_RUN_LOG (RUN_ID, BUSINESS_DATE, TABLE_NAME, OPERATION, ROW_COUNT, NOTE) "
            f"values ('{run_id}', '{d}', '{tbl}', '{op}', {cnt}, '{note}')")
    wm = {r['FEED']: r['LAST_BUSINESS_DATE'] for r in sql("select FEED, LAST_BUSINESS_DATE from INGEST.FEED_WATERMARK")}
    def bump(feed):
        sql(f"update INGEST.FEED_WATERMARK set LAST_BUSINESS_DATE = '{d}', UPDATED_AT = current_timestamp() where FEED = '{feed}'")

    out = []
    # ------------------------------------------------------------- daily feeds
    if d > wm['DAILY']:
        base = dt.date(2026, 9, 1)
        src = base + dt.timedelta(days=(d - base).days % 28)          # same weekday, real September day
        k = (d - src).days                                              # shift in days
        nxt, srcn = d + dt.timedelta(days=1), src + dt.timedelta(days=1)
        late_pct = 1 + d.toordinal() % 3                                # 1-3 % late rows per day
        # day windows per timestamp convention (see macros/tz.sql)
        # Day boundaries follow each feed's own convention (verified against the September data):
        #   Manila day  : alarms, tickets, NPS, campaigns, outages, notes (TIMESTAMP_TZ/LTZ)
        #   LA day      : CDR voice/SMS, XDR (mixed-offset TZ feeds cut on the mediation server's day)
        #   NTZ day     : CDR data, cell PM, ONT, recharge, work orders (TIMESTAMP_NTZ)
        TZ   = lambda x: f"'{x} 00:00:00 +08:00'::timestamp_tz"
        TZLA = lambda x: f"'{x} 00:00:00 {la_off(x)}'::timestamp_tz"
        NTZU = lambda x: f"'{x} 00:00:00'::timestamp_ntz"
        NTZL = lambda x: f"'{x} 00:00:00'::timestamp_ntz"
        # From CUTOVER on, every feed is cut on the MANILA business day, so a run at 01:10
        # Manila never writes events later than "now". The cutover day starts where the
        # LA-day / UTC-day backfill ended (floors below), so there is no overlap and no gap.
        CUTOVER = dt.date(2026, 11, 21)
        manila = d >= CUTOVER
        if manila:
            TZLA = TZ                                                                    # voice / SMS / XDR
            NTZU = lambda x: f"'{x - dt.timedelta(days=1)} 16:00:00'::timestamp_ntz"     # UTC NTZ: Manila day in UTC
        floor_tz  = (lambda col: f" and dateadd('day', {k}, {col}) >= '2026-11-21 00:00:00 -08:00'::timestamp_tz") if manila else (lambda col: "")
        floor_ntz = (lambda col: f" and dateadd('day', {k}, {col}) >= '2026-11-21 00:00:00'::timestamp_ntz") if manila else (lambda col: "")
        late = lambda col, key: (f"dateadd('day', {k} - iff(mod(abs(hash({key}, '{d}')), 100) < {late_pct}, "
                                 f"1 + mod(abs(hash({key})), 5), 0), {col})")
        stage = f"@INGEST.STG_FEEDS"
        ff = "file_format = (format_name = 'INGEST.FF_CSV_GZ') header = false overwrite = true max_file_size = 64000000"
        TSZ = "'YYYY-MM-DD\"T\"HH24:MI:SS.FF3TZH:TZM'"
        TSN = "'YYYY-MM-DD\"T\"HH24:MI:SS.FF3'"

        # --- files for Snowpipe: voice / data / sms / xdr (with late + duplicate rows)
        r = sql(f"""copy into {stage}/cdr_voice/{d}/{run_id}/voice_{d} from (
            with b as (select (select max(call_id) from USAGE.CDR_VOICE) + row_number() over (order by v.call_id) as call_id,
                              v.subscription_id, '639' || lpad(mod(v.subscription_id * 7919, 1000000000)::varchar, 9, '0') as calling,
                              v.called_msisdn, v.cell_id, {late('v.call_start', 'v.call_id')} as ts, v.duration_sec, v.is_roaming,
                              v.roaming_country, v.charge_php, v.call_type, mod(abs(hash(v.call_id, 'dup')), 1000) as h
                         from USAGE.CDR_VOICE v where v.call_start >= {TZLA(src)} and v.call_start < {TZLA(srcn)}{floor_tz('v.call_start')})
            select call_id, subscription_id, calling, called_msisdn, cell_id, to_varchar(ts, {TSZ}), duration_sec, is_roaming,
                   roaming_country, charge_php, call_type from b
            union all
            select call_id, subscription_id, calling, called_msisdn, cell_id, to_varchar(ts, {TSZ}), duration_sec, is_roaming,
                   roaming_country, charge_php, call_type from b where h < 3) {ff}""")
        log('USAGE.CDR_VOICE', 'FILE_ROWS', n(r)); out.append(f"voice_file={n(r)}")

        r = sql(f"""copy into {stage}/cdr_data/{d}/{run_id}/data_{d} from (
            with b as (select (select max(session_id) from USAGE.CDR_DATA) + row_number() over (order by c.session_id) as session_id,
                              c.subscription_id, c.cell_id, {late('c.session_start', 'c.session_id')} as s0,
                              timestampdiff('millisecond', c.session_start, c.session_end) as dur_ms,
                              c.bytes_up, c.bytes_down, c.cumulative_bytes, c.apn, c.rat, c.is_roaming,
                              mod(abs(hash(c.session_id, 'dup')), 1000) as h
                         from USAGE.CDR_DATA c where c.session_start >= {NTZU(src)} and c.session_start < {NTZU(srcn)}{floor_ntz('c.session_start')})
            select session_id, subscription_id, cell_id, to_varchar(s0, {TSN}), to_varchar(dateadd('millisecond', dur_ms, s0), {TSN}),
                   bytes_up, bytes_down, cumulative_bytes, apn, rat, is_roaming from b
            union all
            select session_id, subscription_id, cell_id, to_varchar(s0, {TSN}), to_varchar(dateadd('millisecond', dur_ms, s0), {TSN}),
                   bytes_up, bytes_down, cumulative_bytes, apn, rat, is_roaming from b where h < 3) {ff}""")
        log('USAGE.CDR_DATA', 'FILE_ROWS', n(r)); out.append(f"data_file={n(r)}")

        r = sql(f"""copy into {stage}/cdr_sms/{d}/{run_id}/sms_{d} from (
            with b as (select (select max(sms_id) from USAGE.CDR_SMS) + row_number() over (order by m.sms_id) as sms_id,
                              m.subscription_id, '639' || lpad(mod(m.subscription_id * 7919, 1000000000)::varchar, 9, '0') as sender,
                              m.recipient_msisdn, m.cell_id, {late('m.sent_at', 'm.sms_id')} as ts, m.is_roaming, m.sms_type, m.charge_php,
                              mod(abs(hash(m.sms_id, 'dup')), 1000) as h
                         from USAGE.CDR_SMS m where m.sent_at >= {TZLA(src)} and m.sent_at < {TZLA(srcn)}{floor_tz('m.sent_at')})
            select sms_id, subscription_id, sender, recipient_msisdn, cell_id, to_varchar(ts, {TSZ}), is_roaming, sms_type, charge_php from b
            union all
            select sms_id, subscription_id, sender, recipient_msisdn, cell_id, to_varchar(ts, {TSZ}), is_roaming, sms_type, charge_php from b where h < 3) {ff}""")
        log('USAGE.CDR_SMS', 'FILE_ROWS', n(r)); out.append(f"sms_file={n(r)}")

        r = sql(f"""copy into {stage}/xdr/{d}/{run_id}/xdr_{d} from (
            select object_construct(
                     'RECORD_ID', (select max(record_id) from USAGE.XDR_MEDIATION_RAW where record_id < 1000000000000) + row_number() over (order by x.record_id),
                     'SOURCE_SYSTEM', x.source_system,
                     'RECEIVED_AT', to_varchar(dateadd('day', {k}, x.received_at), {TSZ}),
                     'PAYLOAD', iff(x.payload:start_epoch_ms is null, x.payload,
                                    object_insert(x.payload, 'start_epoch_ms', x.payload:start_epoch_ms::number + {k} * 86400000, true)),
                     'PARSE_STATUS', x.parse_status)
              from USAGE.XDR_MEDIATION_RAW x
             where x.received_at >= {TZLA(src)} and x.received_at < {TZLA(srcn)} and x.record_id < 1000000000000{floor_tz('x.received_at')})
            file_format = (format_name = 'INGEST.FF_JSON_GZ') overwrite = true max_file_size = 64000000""")
        log('USAGE.XDR_MEDIATION_RAW', 'FILE_ROWS', n(r)); out.append(f"xdr_file={n(r)}")

        # --- direct DML feeds (BSS/OSS systems write straight to their tables)
        def ins(tbl, stmt, op='INSERT'):
            c = n(sql(stmt)); log(tbl, op, c); out.append(f"{tbl.split('.')[1].lower()}={c}"); return c

        ins('BILLING.DAILY_REVENUE', f"""insert into BILLING.DAILY_REVENUE (SUBSCRIPTION_ID, BUSINESS_DATE, REVENUE_SOURCE, REVENUE_PHP)
            select subscription_id, '{d}', revenue_source, revenue_php from BILLING.DAILY_REVENUE where business_date = '{src}'""")
        ins('USAGE.USAGE_DAILY_AGG', f"""insert into USAGE.USAGE_DAILY_AGG (SUBSCRIPTION_ID, USAGE_DATE, DATA_MB, VOICE_MIN, SMS_COUNT, SESSION_COUNT)
            select subscription_id, '{d}', data_mb, voice_min, sms_count, session_count from USAGE.USAGE_DAILY_AGG where usage_date = '{src}'""")
        ins('USAGE.ROAMING_USAGE', f"""insert into USAGE.ROAMING_USAGE (SUBSCRIPTION_ID, VISITED_COUNTRY, PARTNER_OPERATOR, USAGE_DATE, DATA_MB, VOICE_MIN, SMS_COUNT, CHARGE_USD, CHARGE_PHP)
            select subscription_id, visited_country, partner_operator, '{d}', data_mb, voice_min, sms_count, charge_usd, charge_php
              from USAGE.ROAMING_USAGE where usage_date = '{src}'""")
        ins('BILLING.RECHARGE', f"""insert into BILLING.RECHARGE (RECHARGE_ID, SUBSCRIPTION_ID, RECHARGED_AT, PACK_CODE, AMOUNT, CHANNEL)
            select (select max(recharge_id) from BILLING.RECHARGE) + row_number() over (order by r.recharge_id), r.subscription_id,
                   {late('r.recharged_at', 'r.recharge_id')}, r.pack_code, r.amount, r.channel
              from BILLING.RECHARGE r where r.recharged_at >= {NTZL(src)} and r.recharged_at < {NTZL(srcn)}""")
        ins('NETWORK.CELL_PERFORMANCE_15MIN', f"""insert into NETWORK.CELL_PERFORMANCE_15MIN
                (CELL_ID, INTERVAL_START, PRB_UTILIZATION_PCT, AVG_THROUGHPUT_MBPS, LATENCY_MS, DROP_CALL_RATE, ACTIVE_USERS)
            select cell_id, dateadd('day', {k}, interval_start), prb_utilization_pct, avg_throughput_mbps, latency_ms, drop_call_rate, active_users
              from NETWORK.CELL_PERFORMANCE_15MIN where interval_start >= {NTZU(src)} and interval_start < {NTZU(srcn)}{floor_ntz('interval_start')}""")
        ins('NETWORK.ONT_TELEMETRY', f"""insert into NETWORK.ONT_TELEMETRY (ONT_SERIAL, POLLED_AT, OPTICAL_RX_DBM, OPTICAL_TX_DBM, TEMPERATURE_C, UPTIME_SEC, COUNTERS)
            select ont_serial, dateadd('day', {k}, polled_at), optical_rx_dbm, optical_tx_dbm, temperature_c, uptime_sec + {k} * 86400, counters
              from NETWORK.ONT_TELEMETRY where polled_at >= {NTZU(src)} and polled_at < {NTZU(srcn)}{floor_ntz('polled_at')}""")

        # outages + impact (restored_at withheld until it happens)
        sql(f"""insert into INGEST.REPLAY_MAP select 'INCIDENT', incident_id,
                   (select max(incident_id) from NETWORK.OUTAGE_INCIDENT) + row_number() over (order by incident_id), {k}, '{d}'
              from NETWORK.OUTAGE_INCIDENT where detected_at >= {TZ(src)} and detected_at < {TZ(srcn)}""")
        ins('NETWORK.OUTAGE_INCIDENT', f"""insert into NETWORK.OUTAGE_INCIDENT (INCIDENT_ID, SEVERITY, REGION_ID, DETECTED_AT, RESTORED_AT, ROOT_CAUSE, AFFECTED_AREA)
            select m.new_id, o.severity, o.region_id, dateadd('day', {k}, o.detected_at),
                   iff(dateadd('day', {k}, o.restored_at) < {TZ(nxt)}, dateadd('day', {k}, o.restored_at), null), o.root_cause, o.affected_area
              from NETWORK.OUTAGE_INCIDENT o join INGEST.REPLAY_MAP m on m.entity = 'INCIDENT' and m.src_id = o.incident_id and m.business_date = '{d}'""")
        ins('NETWORK.OUTAGE_IMPACT', f"""insert into NETWORK.OUTAGE_IMPACT (INCIDENT_ID, SUBSCRIPTION_ID, IMPACT_MINUTES, CREDIT_ELIGIBLE)
            select m.new_id, i.subscription_id, i.impact_minutes, i.credit_eligible
              from NETWORK.OUTAGE_IMPACT i join INGEST.REPLAY_MAP m on m.entity = 'INCIDENT' and m.src_id = i.incident_id and m.business_date = '{d}'""")

        # alarms (cleared_at withheld until it happens)
        sql(f"""insert into INGEST.REPLAY_MAP select 'ALARM', alarm_id,
                   (select max(alarm_id) from NETWORK.ALARM_EVENT) + row_number() over (order by alarm_id), {k}, '{d}'
              from NETWORK.ALARM_EVENT where raised_at >= {TZ(src)} and raised_at < {TZ(srcn)}""")
        ins('NETWORK.ALARM_EVENT', f"""insert into NETWORK.ALARM_EVENT (ALARM_ID, ELEMENT_ID, CELL_ID, ALARM_CODE, SEVERITY, RAISED_AT, CLEARED_AT, INCIDENT_ID, DETAILS, DESCRIPTION)
            select m.new_id, a.element_id, a.cell_id, a.alarm_code, a.severity, dateadd('day', {k}, a.raised_at),
                   iff(dateadd('day', {k}, a.cleared_at) < {TZ(nxt)}, dateadd('day', {k}, a.cleared_at), null),
                   im.new_id, a.details, a.description
              from NETWORK.ALARM_EVENT a
              join INGEST.REPLAY_MAP m on m.entity = 'ALARM' and m.src_id = a.alarm_id and m.business_date = '{d}'
              left join INGEST.REPLAY_MAP im on im.entity = 'INCIDENT' and im.src_id = a.incident_id and im.business_date = '{d}'""")

        # care: tickets open as OPEN; their lifecycle is replayed day by day below
        sql(f"""insert into INGEST.REPLAY_MAP select 'TICKET', ticket_id,
                   (select max(ticket_id) from CARE.TICKET) + row_number() over (order by ticket_id), {k}, '{d}'
              from CARE.TICKET where opened_at >= {TZ(src)} and opened_at < {TZ(srcn)}""")
        ins('CARE.TICKET', f"""insert into CARE.TICKET (TICKET_ID, SUBSCRIPTION_ID, OPENED_AT, CHANNEL, CATEGORY, PRIORITY, STATUS, SUBJECT, RESOLVED_AT)
            select m.new_id, t.subscription_id, dateadd('day', {k}, t.opened_at), t.channel, t.category, t.priority, 'OPEN', t.subject, null
              from CARE.TICKET t join INGEST.REPLAY_MAP m on m.entity = 'TICKET' and m.src_id = t.ticket_id and m.business_date = '{d}'""")
        ins('CARE.TICKET_STATUS_HISTORY', f"""insert into CARE.TICKET_STATUS_HISTORY (TICKET_ID, STATUS, CHANGED_AT, CHANGED_BY)
            select m.new_id, 'OPEN', convert_timezone('Asia/Manila', dateadd('day', {k}, t.opened_at))::timestamp_ntz, 'system'
              from CARE.TICKET t join INGEST.REPLAY_MAP m on m.entity = 'TICKET' and m.src_id = t.ticket_id and m.business_date = '{d}'""")
        ins('CARE.TICKET_INTAKE', f"""insert into CARE.TICKET_INTAKE (TICKET_ID, SUBSCRIPTION_ID, RECEIVED_AT, CHANNEL, RAW_TEXT)
            select m.new_id, i.subscription_id, dateadd('day', {k}, i.received_at), i.channel, i.raw_text
              from CARE.TICKET_INTAKE i join INGEST.REPLAY_MAP m on m.entity = 'TICKET' and m.src_id = i.ticket_id and m.business_date = '{d}'""")
        ins('CARE.NPS_SURVEY', f"""insert into CARE.NPS_SURVEY (SURVEY_ID, SUBSCRIPTION_ID, TICKET_ID, RESPONDED_AT, SCORE, VERBATIM, ANSWERS)
            select (select max(survey_id) from CARE.NPS_SURVEY) + row_number() over (order by s.survey_id), s.subscription_id,
                   m.new_id, dateadd('day', {k}, s.responded_at), s.score, s.verbatim, s.answers
              from CARE.NPS_SURVEY s left join INGEST.REPLAY_MAP m on m.entity = 'TICKET' and m.src_id = s.ticket_id and m.business_date = '{d}'
             where s.responded_at >= {TZ(src)} and s.responded_at < {TZ(srcn)}""")
        ins('CARE.WORK_ORDER', f"""insert into CARE.WORK_ORDER (WORK_ORDER_ID, TICKET_ID, TECHNICIAN_ID, WORK_TYPE, CREATED_AT, DISPATCHED_AT, ARRIVED_AT, COMPLETED_AT, OUTCOME)
            select (select max(work_order_id) from CARE.WORK_ORDER) + row_number() over (order by w.work_order_id), m.new_id, w.technician_id,
                   w.work_type, dateadd('day', {k}, w.created_at), dateadd('day', {k}, w.dispatched_at), dateadd('day', {k}, w.arrived_at),
                   dateadd('day', {k}, w.completed_at), w.outcome
              from CARE.WORK_ORDER w left join INGEST.REPLAY_MAP m on m.entity = 'TICKET' and m.src_id = w.ticket_id and m.business_date = '{d}'
             where w.created_at >= {NTZL(src)} and w.created_at < {NTZL(srcn)}""")
        ins('MARKETING.CAMPAIGN_RESPONSE', f"""insert into MARKETING.CAMPAIGN_RESPONSE (CAMPAIGN_ID, SUBSCRIPTION_ID, SENT_AT, OPENED_AT, CONVERTED, RESPONSE_PAYLOAD)
            select campaign_id, subscription_id, dateadd('day', {k}, sent_at), dateadd('day', {k}, opened_at), converted, response_payload
              from MARKETING.CAMPAIGN_RESPONSE where sent_at >= {TZ(src)} and sent_at < {TZ(srcn)}""")

        # ------------------------------------------------- lifecycle UPDATES due today
        ins('CARE.TICKET_STATUS_HISTORY', f"""insert into CARE.TICKET_STATUS_HISTORY (TICKET_ID, STATUS, CHANGED_AT, CHANGED_BY)
            select m.new_id, h.status, dateadd('day', m.shift_days, h.changed_at), h.changed_by
              from CARE.TICKET_STATUS_HISTORY h join INGEST.REPLAY_MAP m on m.entity = 'TICKET' and m.src_id = h.ticket_id
             where h.status <> 'OPEN' and dateadd('day', m.shift_days, h.changed_at) >= {NTZL(d)}
               and dateadd('day', m.shift_days, h.changed_at) < {NTZL(nxt)}""", 'INSERT_LIFECYCLE')
        ins('CARE.INTERACTION_NOTE', f"""insert into CARE.INTERACTION_NOTE (NOTE_ID, TICKET_ID, CREATED_AT, AUTHOR, NOTE_TEXT, SENTIMENT_SCORE)
            select (select max(note_id) from CARE.INTERACTION_NOTE) + row_number() over (order by n.note_id), m.new_id,
                   dateadd('day', m.shift_days, n.created_at), n.author,
                   'Follow-up logged on ticket ' || m.new_id || ' (' || to_varchar(dateadd('day', m.shift_days, n.created_at), 'YYYY-MM-DD') || ')',
                   n.sentiment_score
              from CARE.INTERACTION_NOTE n join INGEST.REPLAY_MAP m on m.entity = 'TICKET' and m.src_id = n.ticket_id
             where dateadd('day', m.shift_days, n.created_at) >= {TZ(d)} and dateadd('day', m.shift_days, n.created_at) < {TZ(nxt)}""")
        ins('CARE.TICKET', f"""update CARE.TICKET t
               set status = x.cur,
                   resolved_at = iff(x.cur in ('RESOLVED', 'CLOSED'), x.res, null)
              from (select m.new_id, max_by(h.status, h.changed_at) as cur,
                           any_value(iff(dateadd('day', m.shift_days, s.resolved_at) < {TZ(nxt)},
                                         dateadd('day', m.shift_days, s.resolved_at), null)) as res
                      from INGEST.REPLAY_MAP m
                      join CARE.TICKET s on s.ticket_id = m.src_id
                      join CARE.TICKET_STATUS_HISTORY h on h.ticket_id = m.new_id
                     where m.entity = 'TICKET' and m.business_date >= dateadd('day', -60, '{d}')
                     group by m.new_id) x
             where t.ticket_id = x.new_id
               and (t.status <> x.cur or not equal_null(t.resolved_at, iff(x.cur in ('RESOLVED', 'CLOSED'), x.res, null)))""", 'UPDATE')
        ins('NETWORK.ALARM_EVENT', f"""update NETWORK.ALARM_EVENT a set cleared_at = dateadd('day', m.shift_days, s.cleared_at)
              from INGEST.REPLAY_MAP m, NETWORK.ALARM_EVENT s
             where m.entity = 'ALARM' and s.alarm_id = m.src_id and a.alarm_id = m.new_id and a.cleared_at is null
               and m.business_date >= dateadd('day', -30, '{d}')
               and dateadd('day', m.shift_days, s.cleared_at) < {TZ(nxt)}""", 'UPDATE')
        ins('NETWORK.OUTAGE_INCIDENT', f"""update NETWORK.OUTAGE_INCIDENT o set restored_at = dateadd('day', m.shift_days, s.restored_at)
              from INGEST.REPLAY_MAP m, NETWORK.OUTAGE_INCIDENT s
             where m.entity = 'INCIDENT' and s.incident_id = m.src_id and o.incident_id = m.new_id and o.restored_at is null
               and dateadd('day', m.shift_days, s.restored_at) < {TZ(nxt)}""", 'UPDATE')

        # subscriber base movements: plan changes, churn, win-back, suspensions (+ status history)
        moves = f"""
            with pick as (
                select subscription_id, status, mod(abs(hash(subscription_id, '{d}')), 10000) as h from CORE.SUBSCRIPTION)
            select subscription_id, status,
                   case when status = 'ACTIVE'    and h < 3  then 'CHURNED'
                        when status = 'CHURNED'   and h < 8  then 'ACTIVE'
                        when status = 'ACTIVE'    and h between 3 and 4 then 'SUSPENDED'
                        when status = 'SUSPENDED' and h < 250 then 'ACTIVE' end as new_status
              from pick"""
        sql("delete from INGEST.WRK_MOVES")
        sql(f"insert into INGEST.WRK_MOVES select * from ({moves}) where new_status is not null")
        ins('CORE.SUBSCRIPTION_STATUS_HISTORY', f"""update CORE.SUBSCRIPTION_STATUS_HISTORY h set valid_to = '{d} 10:00:00'::timestamp_ntz
              from INGEST.WRK_MOVES m where h.subscription_id = m.subscription_id and h.valid_to is null""", 'UPDATE')
        ins('CORE.SUBSCRIPTION_STATUS_HISTORY', f"""insert into CORE.SUBSCRIPTION_STATUS_HISTORY (SUBSCRIPTION_ID, STATUS, VALID_FROM, VALID_TO)
            select subscription_id, new_status, '{d} 10:00:00'::timestamp_ntz, null from INGEST.WRK_MOVES""")
        ins('CORE.SUBSCRIPTION', f"""update CORE.SUBSCRIPTION s set status = m.new_status,
                   deactivated_at = iff(m.new_status = 'CHURNED', '{d}'::date, iff(m.new_status = 'ACTIVE', null, s.deactivated_at))
              from INGEST.WRK_MOVES m where s.subscription_id = m.subscription_id""", 'UPDATE')
        ins('CORE.SUBSCRIPTION', f"""update CORE.SUBSCRIPTION t set plan_id = x.new_plan
              from (select s.subscription_id, p.plan_id as new_plan
                      from (select subscription_id from CORE.SUBSCRIPTION where status = 'ACTIVE'
                             order by hash(subscription_id, '{d}', 'plan') limit 400) s
                      join (select plan_id, row_number() over (order by plan_id) - 1 as rn, count(*) over () as cnt from REF.PLAN_CATALOG) p
                        on p.rn = mod(abs(hash(s.subscription_id, '{d}')), p.cnt)) x
             where t.subscription_id = x.subscription_id and t.plan_id <> x.new_plan""", 'UPDATE_PLAN')
        ins('CORE.CUSTOMER', f"""update CORE.CUSTOMER set preferred_language = iff(preferred_language = 'EN', 'FIL', 'EN')
             where mod(abs(hash(customer_id, '{d}')), 10000) < 2""", 'UPDATE')
        ins('CORE.ACCOUNT', f"""update CORE.ACCOUNT set payment_method = case mod(abs(hash(account_id, '{d}')), 3)
                   when 0 then 'GCASH' when 1 then 'CREDIT_CARD' else 'BANK_TRANSFER' end, paperless_billing = not paperless_billing
             where mod(abs(hash(account_id, '{d}', 'acct')), 10000) < 2""", 'UPDATE')

        # ------------------------------------------------- hard DELETES (mediation voids)
        prev = d - dt.timedelta(days=1)
        if prev > dt.date(2026, 9, 30):
            ins('USAGE.CDR_VOICE', f"""delete from USAGE.CDR_VOICE where call_start >= {TZLA(prev)} and call_start < {TZLA(d)}
                   and mod(abs(hash(call_id, 'void')), 10000) < 5""", 'DELETE')
            ins('USAGE.CDR_DATA', f"""delete from USAGE.CDR_DATA where session_start >= {NTZU(prev)} and session_start < {NTZU(d)}
                   and mod(abs(hash(session_id, 'void')), 10000) < 5""", 'DELETE')
            ins('USAGE.CDR_SMS', f"""delete from USAGE.CDR_SMS where sent_at >= {TZLA(prev)} and sent_at < {TZLA(d)}
                   and mod(abs(hash(sms_id, 'void')), 10000) < 5""", 'DELETE')
        # vendor + partner files for the external tables (s3://.../ingest/, deterministic, overwrite)
        vf = sql(f"call INGEST.SP_WRITE_VENDOR_FILES('{d}')")[0][0]
        log('INGEST.EXT_DEVICE_FEED / EXT_PARTNER_RATE', 'S3_FILES', 2, vf); out.append(f"s3_files({vf})")
        log('ALL', 'LATE_PCT', late_pct, f'src={src} shift={k}')
        bump('DAILY')

    # ------------------------------------------------------------- billing follow-ups (month-shift replay)
    s = (dt.date(d.year, d.month, 1) - dt.timedelta(days=1)).replace(day=1)   # first of previous month
    try:
        sd = s.replace(day=d.day)                                              # same day-of-month, previous month
    except ValueError:
        sd = None
    if sd:
        ms = (d - sd).days
        remap = f"""left join BILLING.INVOICE si on si.invoice_id = x.invoice_id
                    left join BILLING.INVOICE ni on ni.account_id = x.account_id and ni.billing_month = dateadd('month', 1, si.billing_month)"""
        if d > wm['PAYMENT']:
            r = sql(f"""copy into @INGEST.STG_FEEDS/payment/{d}/{run_id}/payment_{d} from (
                select (select max(payment_id) from BILLING.PAYMENT) + row_number() over (order by x.payment_id), x.account_id, ni.invoice_id,
                       to_varchar(dateadd('day', {ms}, x.paid_at), 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZH:TZM'), x.amount, x.payment_method
                  from BILLING.PAYMENT x {remap}
                 where x.paid_at >= '{sd} 00:00:00 +08:00'::timestamp_tz and x.paid_at < dateadd('day', 1, '{sd} 00:00:00 +08:00'::timestamp_tz))
                file_format = (format_name = 'INGEST.FF_CSV_GZ') header = false overwrite = true""")
            log('BILLING.PAYMENT', 'FILE_ROWS', n(r)); out.append(f"payment_file={n(r)}")
            prev = d - dt.timedelta(days=1)
            c = n(sql(f"""delete from BILLING.PAYMENT where paid_at >= '{prev} 00:00:00 +08:00'::timestamp_tz
                           and paid_at < '{d} 00:00:00 +08:00'::timestamp_tz
                           and mod(abs(hash(payment_id, 'reversal')), 1000) < 1"""))
            log('BILLING.PAYMENT', 'DELETE', c, 'payment reversals')
            bump('PAYMENT')
        if d > wm['ADJUSTMENT']:
            c = n(sql(f"""insert into BILLING.ADJUSTMENT (ADJUSTMENT_ID, INVOICE_ID, ACCOUNT_ID, ADJUSTED_AT, AMOUNT, REASON_CODE)
                select (select max(adjustment_id) from BILLING.ADJUSTMENT) + row_number() over (order by x.adjustment_id),
                       ni.invoice_id, x.account_id, '{d}', x.amount, x.reason_code
                  from BILLING.ADJUSTMENT x {remap} where x.adjusted_at = '{sd}'"""))
            log('BILLING.ADJUSTMENT', 'INSERT', c); out.append(f"adjustment={c}"); bump('ADJUSTMENT')
        if d > wm['DUNNING']:
            c = n(sql(f"""insert into BILLING.DUNNING_EVENT (DUNNING_ID, ACCOUNT_ID, INVOICE_ID, DUNNING_STAGE, ACTION, ACTION_AT, AMOUNT_DUE)
                select (select max(dunning_id) from BILLING.DUNNING_EVENT) + row_number() over (order by x.dunning_id), x.account_id,
                       ni.invoice_id, x.dunning_stage, x.action, dateadd('day', {ms}, x.action_at), x.amount_due
                  from BILLING.DUNNING_EVENT x {remap}
                 where x.action_at >= '{sd} 00:00:00 +08:00'::timestamp_tz and x.action_at < dateadd('day', 1, '{sd} 00:00:00 +08:00'::timestamp_tz)"""))
            log('BILLING.DUNNING_EVENT', 'INSERT', c); out.append(f"dunning={c}"); bump('DUNNING')

    # ------------------------------------------------------------- bill cycle on the last day of the month
    last = (dt.date(d.year + d.month // 12, d.month % 12 + 1, 1) - dt.timedelta(days=1))
    if d == last and d > wm['BILL_CYCLE']:
        bm, sbm = dt.date(d.year, d.month, 1), s
        sql(f"""insert into INGEST.INVOICE_MAP select invoice_id,
                   'G' || lpad(to_varchar((select max(try_to_number(substr(invoice_id, 2))) from BILLING.INVOICE) + row_number() over (order by invoice_id)), 10, '0'),
                   '{bm}' from BILLING.INVOICE where billing_month = '{sbm}'""")
        c = n(sql(f"""insert into BILLING.INVOICE (INVOICE_ID, ACCOUNT_ID, SOURCE_CUSTOMER_REF, BILLING_MONTH, ISSUE_DATE, DUE_DATE, TOTAL_AMOUNT, CURRENCY_CODE, INVOICE_STATUS)
            select m.new_invoice_id, i.account_id, i.source_customer_ref, '{bm}', '{d}', dateadd('day', 15, '{d}'), i.total_amount, i.currency_code, 'UNPAID'
              from BILLING.INVOICE i join INGEST.INVOICE_MAP m on m.src_invoice_id = i.invoice_id and m.billing_month = '{bm}'"""))
        log('BILLING.INVOICE', 'INSERT', c, 'bill cycle'); out.append(f"invoices={c}")
        c = n(sql(f"""insert into BILLING.INVOICE_LINE (INVOICE_ID, LINE_NO, LINE_TYPE, DESCRIPTION, AMOUNT, AMOUNT_PRECISE)
            select m.new_invoice_id, l.line_no, l.line_type, l.description, l.amount, l.amount_precise
              from BILLING.INVOICE_LINE l join INGEST.INVOICE_MAP m on m.src_invoice_id = l.invoice_id and m.billing_month = '{bm}'"""))
        log('BILLING.INVOICE_LINE', 'INSERT', c, 'bill cycle')
        c = n(sql(f"""insert into BILLING.LATE_FEE_ASSESSMENT (INVOICE_ID, ACCOUNT_ID, AS_OF, RULE_CODE, DAYS_OVERDUE, FEE_PHP)
            select coalesce(m.new_invoice_id, f.invoice_id), f.account_id, '{d}', f.rule_code, f.days_overdue, f.fee_php
              from BILLING.LATE_FEE_ASSESSMENT f left join INGEST.INVOICE_MAP m on m.src_invoice_id = f.invoice_id and m.billing_month = '{bm}'
             where f.as_of = (select max(as_of) from BILLING.LATE_FEE_ASSESSMENT)"""))
        log('BILLING.LATE_FEE_ASSESSMENT', 'INSERT', c, 'month-end assessment')
        bump('BILL_CYCLE')

    # ------------------------------------------------------------- daily billing UPDATES
    c = n(sql(f"""update BILLING.PAYMENT p set invoice_id = i.invoice_id
          from BILLING.INVOICE i
         where p.invoice_id is null and p.paid_at >= dateadd('day', -20, '{d}'::timestamp_ltz)
           and i.account_id = p.account_id and i.billing_month = date_trunc('month', dateadd('day', -15, p.paid_at))::date"""))
    log('BILLING.PAYMENT', 'UPDATE', c, 'payment allocation')
    c = n(sql(f"""update BILLING.INVOICE i set invoice_status = 'PAID'
          from (select invoice_id, sum(amount) paid from BILLING.PAYMENT
                 where paid_at >= dateadd('day', -75, '{d}'::timestamp_ltz) and invoice_id is not null group by 1) p
         where i.invoice_id = p.invoice_id and i.invoice_status <> 'PAID' and p.paid >= i.total_amount - 0.005
           and i.billing_month >= dateadd('month', -2, date_trunc('month', '{d}'::date))"""))
    log('BILLING.INVOICE', 'UPDATE', c, 'paid')
    c = n(sql(f"""update BILLING.INVOICE set invoice_status = 'OVERDUE'
         where invoice_status = 'UNPAID' and due_date < '{d}' and billing_month >= dateadd('month', -2, date_trunc('month', '{d}'::date))"""))
    log('BILLING.INVOICE', 'UPDATE', c, 'overdue')

    return f"{run_id}: " + ", ".join(out)
$$;

-- Trigger the pipes for one business day and wait until every file is loaded.
CREATE OR REPLACE PROCEDURE INGEST.SP_LOAD_FILES(D DATE)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Snowpipe refresh for the day''s landed files + wait + COPY_HISTORY reconciliation into OPS.INGEST_RUN_LOG'
EXECUTE AS OWNER
AS
$$
import json, time, datetime as dt

PIPES = {'PIPE_CDR_VOICE': ('USAGE.CDR_VOICE', 'cdr_voice'), 'PIPE_CDR_DATA': ('USAGE.CDR_DATA', 'cdr_data'),
         'PIPE_CDR_SMS': ('USAGE.CDR_SMS', 'cdr_sms'), 'PIPE_XDR': ('USAGE.XDR_MEDIATION_RAW', 'xdr'),
         'PIPE_PAYMENT': ('BILLING.PAYMENT', 'payment')}

def run(session, d):
    if isinstance(d, str):
        d = dt.date.fromisoformat(d)
    start = session.sql("select current_timestamp()").collect()[0][0]
    # Each ingest attempt writes to <date>/<run_id>/; only the run that COMMITTED (and therefore
    # logged FILE_ROWS) is loaded, so files from a rolled-back attempt can never reach bronze.
    run_id = session.sql(f"""select max_by(run_id, logged_at) from OPS.INGEST_RUN_LOG
                              where business_date = '{d}' and operation = 'FILE_ROWS' and run_id like 'ING-%'""").collect()[0][0]
    if run_id is None:
        return f"no files to load for {d}"
    for p, (_, folder) in PIPES.items():
        session.sql(f"alter pipe INGEST.{p} refresh prefix = '{d}/{run_id}/'").collect()
    def loaded(p, tbl):
        r = session.sql(f"""select coalesce(sum(row_count), 0), count(*), coalesce(count_if(status <> 'Loaded'), 0)
                              from table(TELCO_PROD_XL_V2.information_schema.copy_history(
                                   table_name => 'TELCO_PROD_XL_V2.{tbl}', start_time => dateadd('day', -7, '{start}'::timestamp_ltz)))
                             where endswith(pipe_name, '{p}') and startswith(file_name, '{d}/{run_id}/')""").collect()[0]
        return int(r[0] or 0), int(r[1] or 0), int(r[2] or 0)
    expected = {}
    for p, (tbl, _) in PIPES.items():
        expected[p] = int(session.sql(f"""select coalesce(sum(row_count), 0) from OPS.INGEST_RUN_LOG
                               where run_id = '{run_id}' and table_name = '{tbl}' and operation = 'FILE_ROWS'""").collect()[0][0] or 0)
    # Snowpipe is asynchronous and COPY_HISTORY lags a little: poll until every pipe has
    # loaded exactly the rows the ingestor exported (or a file errors / 25 min pass).
    deadline = time.time() + 1500
    state = {}
    while True:
        state = {p: loaded(p, tbl) for p, (tbl, _) in PIPES.items()}
        if any(v[2] for v in state.values()):
            break
        if all(state[p][0] == expected[p] for p in PIPES) or time.time() > deadline:
            break
        time.sleep(15)
    out = []
    for p, (tbl, _) in PIPES.items():
        rows, files, bad = state[p]
        exp = expected[p]
        session.sql(f"""insert into OPS.INGEST_RUN_LOG (RUN_ID, BUSINESS_DATE, TABLE_NAME, OPERATION, ROW_COUNT, NOTE)
                        values ('LOAD-' || '{run_id}', '{d}', '{tbl}', 'SNOWPIPE_LOAD', {rows}, 'files={files} not_loaded={bad} exported={exp} pipe={p}')""").collect()
        out.append(f"{tbl.split('.')[1].lower()}={rows}({files} files)")
        if bad:
            raise Exception(f"{p}: {bad} file(s) not loaded for {d}")
        if rows != exp:
            raise Exception(f"{p}: loaded {rows} rows but the ingestor exported {exp} for {d}")
    # external tables: auto-refresh via S3 event notifications; refresh the day's partition
    # explicitly as well so the run never depends on notification latency
    for ext in ('INGEST.EXT_DEVICE_FEED', 'INGEST.EXT_PARTNER_RATE'):
        session.sql(f"alter external table {ext} refresh 'dt={d}/'").collect()
        n = int(session.sql(f"select count(*) from {ext} where feed_date = '{d}'").collect()[0][0])
        session.sql(f"""insert into OPS.INGEST_RUN_LOG (RUN_ID, BUSINESS_DATE, TABLE_NAME, OPERATION, ROW_COUNT, NOTE)
                        values ('LOAD-' || '{run_id}', '{d}', '{ext}', 'EXTERNAL_TABLE_ROWS', {n}, 'refreshed partition dt={d}')""").collect()
        out.append(f"{ext.split('.')[1].lower()}={n}")
    return f"loaded {d}: " + ", ".join(out)
$$;

GRANT USAGE ON PROCEDURE INGEST.SP_INGEST_DAY(DATE) TO ROLE DBT_ROLE;
GRANT USAGE ON PROCEDURE INGEST.SP_LOAD_FILES(DATE) TO ROLE DBT_ROLE;
