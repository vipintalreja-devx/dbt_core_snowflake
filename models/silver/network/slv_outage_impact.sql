{{ config(
    materialized='incremental',
    schema='silver',
    tags=['silver', 'network', 'cdc'],
    transient=true,
    unique_key=['incident_id', 'subscription_id'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_purge_tombstones() }}"]
) }}
-- Subscribers impacted per outage. CDC from bronze.
select
    incident_id,
    subscription_id,
    impact_minutes,
    coalesce(credit_eligible, false)                        as is_credit_eligible,
    _cdc_action,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ cdc_source('network', 'outage_impact') }}
qualify row_number() over (partition by incident_id, subscription_id order by iff(_cdc_action = 'DELETE', 1, 0) desc, impact_minutes desc) = 1
