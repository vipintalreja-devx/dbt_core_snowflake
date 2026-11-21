{%- set changed = cdc_source_changed_dates('usage', 'roaming_usage', 'usage_date') -%}
{{ config(
    materialized='incremental',
    schema='silver',
    tags=['silver', 'usage', 'cdc'],
    transient=true,
    unique_key='event_date',
    incremental_strategy='delete+insert',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('event_date', has_action=false) }}"]
) }}
-- TAP roaming usage with USD charges converted to PHP at the FX rate valid on the usage day.
-- No natural key in bronze, so CDC recomputes every usage day that changed.
with ru as (
    select * from {{ source('usage', 'roaming_usage') }}
    where usage_date >= '{{ var("history_start_date") }}'::date
    {%- if changed %} and usage_date in {{ changed }}{% elif is_incremental() %} and {{ incremental_date_filter('usage_date', 'event_date') }}{% endif %}
),
fx as (
    select valid_from, valid_to, rate
    from {{ ref('slv_fx_rate') }}
    where from_currency = 'USD' and to_currency = 'PHP'
)
select
    ru.subscription_id,
    ru.usage_date                                           as event_date,
    upper(ru.visited_country)                               as visited_country,
    ru.partner_operator,
    zeroifnull(ru.data_mb)                                  as data_mb,
    zeroifnull(ru.voice_min)                                as voice_min,
    zeroifnull(ru.sms_count)                                as sms_count,
    ru.charge_usd::number(18, 4)                            as charge_usd,
    ru.charge_php::number(18, 4)                            as charge_php_billed,
    fx.rate                                                 as usd_php_rate,
    round(ru.charge_usd * fx.rate, 4)                       as charge_php_at_fx,
    round(ru.charge_php - ru.charge_usd * fx.rate, 4)       as fx_variance_php,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from ru
left join fx
       on ru.usage_date >= fx.valid_from
      and ru.usage_date <  fx.valid_to
