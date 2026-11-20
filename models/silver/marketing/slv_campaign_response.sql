{{ config(
    tags=['silver', 'marketing'],
    schema='silver',
    materialized='incremental',
    unique_key='response_id',
    incremental_strategy='merge',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns'
) }}
-- A line can receive the same campaign several times; grain = campaign x line x send time.
select
    {{ dbt_utils.generate_surrogate_key(['campaign_id', 'subscription_id', 'sent_at']) }} as response_id,
    campaign_id,
    subscription_id,
    {{ tz_to_utc('sent_at') }}                              as sent_at_utc,
    {{ utc_to_local(tz_to_utc('sent_at')) }}::date          as event_date,
    {{ tz_to_utc('opened_at') }}                            as opened_at_utc,
    opened_at is not null                                   as is_opened,
    coalesce(converted, false)                              as is_converted,
    round(datediff('minute', sent_at, opened_at) / 60, 2)   as hours_to_open
from {{ source('marketing', 'campaign_response') }}
where {{ incremental_date_filter(utc_to_local(tz_to_utc('sent_at')) ~ '::date', 'event_date') }}
