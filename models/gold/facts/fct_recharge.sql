{{ config(
    tags=['gold', 'facts', 'fact'],
    schema='gold',
    materialized='incremental',
    unique_key='recharge_id',
    incremental_strategy='merge',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns'
) }}
select
    r.recharge_id,
    r.subscription_id,
    r.event_date,
    r.recharged_at_local,
    r.pack_code,
    r.recharge_channel,
    r.recharge_amount_php,
    datediff('day',
             lag(r.event_date) over (partition by r.subscription_id order by r.recharged_at_local),
             r.event_date)                                  as days_since_prev_recharge
from {{ ref('slv_recharge') }} r
where {{ incremental_date_filter('r.event_date', 'event_date') }}
