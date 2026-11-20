{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'network'],
    transient=true
) }}
select
    incident_id,
    subscription_id,
    impact_minutes,
    coalesce(credit_eligible, false)                        as is_credit_eligible
from {{ source('network', 'outage_impact') }}
qualify row_number() over (partition by incident_id, subscription_id order by impact_minutes desc) = 1
