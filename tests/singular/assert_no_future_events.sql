-- No usage may be dated after the load date (catches timezone conversion bugs).
select 'voice' as feed, max(event_date) as max_date from {{ ref('slv_cdr_voice') }} having max(event_date) > current_date
union all
select 'data', max(event_date) from {{ ref('slv_cdr_data') }} having max(event_date) > current_date
union all
select 'sms', max(event_date) from {{ ref('slv_cdr_sms') }} having max(event_date) > current_date
