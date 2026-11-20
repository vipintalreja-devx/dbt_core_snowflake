{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dims', 'dimension'],
    transient=false,
    copy_grants=true,
    post_hook="alter table {{ this }} set comment = 'Gold dimension - dim_technician (rebuilt {{ run_started_at.strftime(\'%Y-%m-%d %H:%M\') }} UTC)'"
) }}
select t.technician_id, t.technician_name, t.region_id, r.region_name, t.skill_count, t.hired_on, t.is_contractor
from {{ ref('slv_field_technician') }} t
left join {{ ref('slv_region') }} r on r.region_id = t.region_id
