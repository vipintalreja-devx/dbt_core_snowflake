{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'partner'],
    transient=true
) }}
select
    partner_brand_id,
    subscription_id,
    {{ tz_to_utc('onboarded_at') }}                         as onboarded_at_utc,
    wholesale_plan
from {{ source('partner', 'partner_subscriber_map') }}
