{%- set changed = cdc_source_changed_dates('billing', 'daily_revenue', 'business_date') -%}
{{ config(
    tags=['silver', 'billing', 'cdc'],
    schema='silver',
    materialized='incremental',
    unique_key='business_date',
    incremental_strategy='delete+insert',
    cluster_by=['business_date'],
    on_schema_change='append_new_columns',
    post_hook=["{{ cdc_log_changed_dates('business_date', has_action=false) }}"]
) }}
-- Earned (recognised) revenue per line, day and revenue stream. ~790M rows in bronze.
-- CDC: only business dates with inserted/updated/deleted bronze rows are recomputed
-- (whole day re-aggregated, so restatements and deletes are exact).
select
    subscription_id,
    business_date,
    upper(revenue_source)                                   as revenue_source,
    sum(revenue_php)::number(18, 4)                         as revenue_php,
    {{ dbt_loaded_at() }}                                   as _dbt_loaded_at
from {{ source('billing', 'daily_revenue') }}
where {% if changed %}business_date in {{ changed }}{% else %}{{ incremental_date_filter('business_date') }}{% endif %}
group by 1, 2, 3
