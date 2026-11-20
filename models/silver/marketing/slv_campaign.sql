{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'marketing'],
    transient=true
) }}
select
    campaign_id,
    campaign_name,
    upper(channel)                                          as channel,
    start_date,
    end_date,
    budget_php::number(18, 2)                               as budget_php,
    targeting_rules,
    coalesce(is_active, false)                              as is_active
from {{ source('marketing', 'campaign') }}
