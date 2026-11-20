{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dims', 'dimension'],
    transient=false,
    copy_grants=true,
    post_hook="alter table {{ this }} set comment = 'Gold dimension - dim_device (rebuilt {{ run_started_at.strftime(\'%Y-%m-%d %H:%M\') }} UTC)'"
) }}
select device_id, manufacturer, model, device_type, supports_5g, launch_date, list_price_php,
       case when list_price_php >= 50000 then 'PREMIUM' when list_price_php >= 15000 then 'MID' else 'ENTRY' end as price_tier
from {{ ref('slv_device') }}
