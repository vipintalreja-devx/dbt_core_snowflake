{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'core'],
    transient=true
) }}
-- Latest consent decision per customer and consent type (DPA compliance).
select
    customer_id,
    upper(consent_type)                                     as consent_type,
    coalesce(granted, false)                                as is_granted,
    channel_list,
    {{ tz_to_utc('captured_at') }}                          as captured_at_utc
from {{ source('core', 'customer_consent') }}
qualify row_number() over (partition by customer_id, upper(consent_type) order by captured_at desc) = 1
