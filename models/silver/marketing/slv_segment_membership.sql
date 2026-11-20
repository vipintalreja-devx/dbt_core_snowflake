{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'marketing'],
    transient=true
) }}
select
    subscription_id,
    upper(segment_code)                                     as segment_code,
    valid_from,
    coalesce(valid_to, '9999-12-31'::date)                  as valid_to,
    valid_to is null                                        as is_current,
    assigned_by
from {{ source('marketing', 'segment_membership') }}
