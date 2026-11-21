{{ config(
    tags=['silver', 'marketing', 'cdc'],
    schema='silver',
    materialized='incremental',
    unique_key='response_id',
    incremental_strategy='merge',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('event_date') }}",
               "{{ cdc_log_deletes('response_id', 'event_date') }}",
               "{{ cdc_purge_tombstones() }}"]
) }}
-- A line can receive the same campaign several times; grain = campaign x line x send time. CDC from bronze.
select
    {{ dbt_utils.generate_surrogate_key(['campaign_id', 'subscription_id', 'sent_at']) }} as response_id,
    campaign_id,
    subscription_id,
    {{ tz_to_utc('sent_at') }}                              as sent_at_utc,
    {{ utc_to_local(tz_to_utc('sent_at')) }}::date          as event_date,
    {{ tz_to_utc('opened_at') }}                            as opened_at_utc,
    opened_at is not null                                   as is_opened,
    coalesce(converted, false)                              as is_converted,
    round(datediff('minute', sent_at, opened_at) / 60, 2)   as hours_to_open,
    _cdc_action,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ cdc_source('marketing', 'campaign_response', "sent_at >= " ~ incremental_ts_lower_bound() ~ "::timestamp_tz") }}
where {{ full_refresh_floor(utc_to_local(tz_to_utc('sent_at')) ~ '::date') }}
qualify row_number() over (partition by campaign_id, subscription_id, sent_at order by iff(_cdc_action = 'DELETE', 1, 0) desc) = 1
