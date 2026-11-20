{{ config(
    tags=['silver', 'billing'],
    schema='silver',
    materialized='incremental',
    unique_key='business_date',
    incremental_strategy='delete+insert',
    cluster_by=['business_date'],
    on_schema_change='append_new_columns'
) }}
-- Earned (recognised) revenue per line, day and revenue stream. ~790M rows in bronze;
-- loaded incrementally by business day with a re-processing window for restatements.
select
    subscription_id,
    business_date,
    upper(revenue_source)                                   as revenue_source,
    sum(revenue_php)::number(18, 4)                         as revenue_php
from {{ source('billing', 'daily_revenue') }}
where {{ incremental_date_filter('business_date') }}
group by 1, 2, 3
