{{ config(
    tags=['silver', 'billing', 'cdc'],
    schema='silver',
    materialized='incremental',
    unique_key='recharge_id',
    incremental_strategy='merge',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('event_date') }}",
               "{{ cdc_log_deletes('recharge_id', 'event_date') }}",
               "{{ cdc_purge_tombstones() }}"]
) }}
-- Prepaid top-ups. Bronze RECHARGED_AT is TIMESTAMP_NTZ in Manila local time. CDC from bronze.
select
    recharge_id,
    subscription_id,
    {{ local_ntz_to_utc('recharged_at') }}                  as recharged_at_utc,
    recharged_at::timestamp_ntz                             as recharged_at_local,
    recharged_at::date                                      as event_date,
    pack_code,
    amount::number(18, 2)                                   as recharge_amount_php,
    upper(trim(channel))                                    as recharge_channel,
    _cdc_action,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ cdc_source('billing', 'recharge', "recharged_at >= " ~ incremental_ts_lower_bound()) }}
where amount > 0
  and {{ full_refresh_floor('recharged_at::date') }}
qualify row_number() over (partition by recharge_id order by iff(_cdc_action = 'DELETE', 1, 0) desc, recharged_at desc) = 1
