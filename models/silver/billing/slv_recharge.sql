{{ config(
    tags=['silver', 'billing'],
    schema='silver',
    materialized='incremental',
    unique_key='recharge_id',
    incremental_strategy='merge',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns'
) }}
-- Prepaid top-ups. Bronze RECHARGED_AT is TIMESTAMP_NTZ in Manila local time.
select
    recharge_id,
    subscription_id,
    {{ local_ntz_to_utc('recharged_at') }}                  as recharged_at_utc,
    recharged_at::timestamp_ntz                             as recharged_at_local,
    recharged_at::date                                      as event_date,
    pack_code,
    amount::number(18, 2)                                   as recharge_amount_php,
    upper(trim(channel))                                    as recharge_channel
from {{ source('billing', 'recharge') }}
where amount > 0
  and {{ incremental_date_filter('recharged_at::date', 'event_date') }}
qualify row_number() over (partition by recharge_id order by recharged_at desc) = 1
