-- No usage may be dated after today's Manila business day (catches timezone conversion bugs
-- and a source feed running ahead of the clock). Filters on the clustering column, so it
-- prunes to nothing on a healthy day instead of scanning full CDR history.
{% set today_manila = "convert_timezone('Asia/Manila', current_timestamp())::date" %}
select 'voice' as feed, event_date from {{ ref('slv_cdr_voice') }} where event_date > {{ today_manila }}
union all
select 'data', event_date from {{ ref('slv_cdr_data') }} where event_date > {{ today_manila }}
union all
select 'sms', event_date from {{ ref('slv_cdr_sms') }} where event_date > {{ today_manila }}
