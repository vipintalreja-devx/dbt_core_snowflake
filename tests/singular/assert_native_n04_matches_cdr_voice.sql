{{ config(severity='warn', store_failures=true, tags=['seam', 'native']) }}
-- Seam test between the native pipelines and the dbt sources (would have caught N04).
-- NATIVE_PIPE.N04 copies USAGE.CDR_VOICE into NATIVE_MART.N04_VOICE_INBOX. CALL_START is a
-- TIMESTAMP_TZ with mixed offsets: a careless TZ -> LTZ / NTZ conversion keeps "a" time but
-- can shift the instant or the business day. Checked over the last 3 days of captures:
--   TZ_SHIFT : same CALL_ID, different instant than the source row
--   FUTURE   : captured call time later than now
--   MISSING  : source CDR ingested > 2 h ago, inside the captured window, never captured
-- Warn-level (native code is owned by the platform team); failures are stored for review.
with inbox as (
    select call_id, call_start, ingested_at, captured_at
    from {{ source('native_mart', 'n04_voice_inbox') }}
    where captured_at >= dateadd('day', -3, current_timestamp())
),
window_ as (
    select min(ingested_at) as from_ts from inbox
),
src as (
    select call_id, call_start, ingested_at
    from {{ source('usage', 'cdr_voice') }}
    where call_start >= dateadd('day', -10, current_timestamp())::timestamp_tz
      and ingested_at >= (select from_ts from window_)
    qualify row_number() over (partition by call_id order by ingested_at desc) = 1
)
select 'TZ_SHIFT' as issue, i.call_id,
       to_varchar(s.call_start) as source_call_start, to_varchar(i.call_start) as native_call_start
from inbox i
join src s on s.call_id = i.call_id
where convert_timezone('UTC', s.call_start) <> convert_timezone('UTC', i.call_start)
union all
select 'FUTURE', i.call_id, null, to_varchar(i.call_start)
from inbox i
where i.call_start > current_timestamp()
union all
select 'MISSING', s.call_id, to_varchar(s.call_start), null
from src s
where s.ingested_at < dateadd('hour', -2, current_timestamp())
  and not exists (select 1 from inbox i where i.call_id = s.call_id)
