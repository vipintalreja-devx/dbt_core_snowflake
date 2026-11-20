{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dims', 'dimension'],
    transient=false,
    copy_grants=true,
    post_hook="alter table {{ this }} set comment = 'Gold dimension - dim_subscriber_history (rebuilt {{ run_started_at.strftime(\'%Y-%m-%d %H:%M\') }} UTC)'"
) }}
-- SCD Type 2 subscriber history from the daily snapshot (plan / status / home cell / device / brand).
select
    {{ dbt_utils.generate_surrogate_key(['s.subscription_id', 's.dbt_valid_from']) }} as subscriber_version_key,
    s.subscription_id,
    s.account_id,
    s.plan_id,
    p.plan_name,
    p.contract_type,
    s.status,
    s.home_cell_id,
    coalesce(c.region_name, 'UNKNOWN')                      as home_region_name,
    s.device_id,
    s.partner_brand_id,
    s.monthly_charges::number(18, 2)                        as monthly_charges_php,
    s.is_deleted,
    s.dbt_valid_from                                        as valid_from,
    coalesce(s.dbt_valid_to, '9999-12-31'::timestamp_ntz)   as valid_to,
    s.dbt_valid_to is null                                  as is_current
from {{ ref('snap_subscription') }} s
left join {{ ref('slv_plan') }}      p on p.plan_id = s.plan_id
left join {{ ref('slv_cell_site') }} c on c.cell_id = s.home_cell_id
