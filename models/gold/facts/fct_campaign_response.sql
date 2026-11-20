{{ config(
    tags=['gold', 'facts', 'fact'],
    schema='gold',
    materialized='incremental',
    unique_key='response_id',
    incremental_strategy='merge',
    cluster_by=['event_date'],
    on_schema_change='append_new_columns'
) }}
select response_id, campaign_id, subscription_id, event_date, is_opened, is_converted, hours_to_open
from {{ ref('slv_campaign_response') }}
where {{ incremental_date_filter('event_date') }}
