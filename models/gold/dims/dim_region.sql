{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dims', 'dimension'],
    transient=false,
    copy_grants=true,
    post_hook="alter table {{ this }} set comment = 'Gold dimension - dim_region (rebuilt {{ run_started_at.strftime(\'%Y-%m-%d %H:%M\') }} UTC)'"
) }}
select region_id, region_name, region_city, island_group, country_code
from {{ ref('slv_region') }}
