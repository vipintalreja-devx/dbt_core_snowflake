{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dims', 'dimension'],
    transient=false,
    copy_grants=true,
    post_hook="alter table {{ this }} set comment = 'Gold dimension - dim_plan (rebuilt {{ run_started_at.strftime(\'%Y-%m-%d %H:%M\') }} UTC)'"
) }}
-- Materialization: table (current-state/small reference): dimension (small, current state; history comes from the SCD2 snapshots).
-- Current tariff plan attributes + current subscriber base on the plan.
with base as (
    select plan_id, count(*) as active_lines
    from {{ ref('slv_subscription') }}
    where is_reportable and status in ('ACTIVE', 'SUSPENDED')
    group by 1
)
select
    p.plan_id,
    p.plan_name,
    p.contract_type,
    p.commitment_months,
    p.plan_category,
    p.internet_service,
    p.base_price_php,
    p.speed_mbps,
    p.data_cap_gb,
    p.is_promo,
    p.is_sellable,
    p.effective_from,
    p.effective_to,
    p.feature_list,
    p.feature_count,
    coalesce(b.active_lines, 0)                             as active_lines,
    ratio_to_report(coalesce(b.active_lines, 0)) over ()    as base_share,
    rank() over (partition by p.contract_type order by p.base_price_php desc) as price_rank_in_contract
from {{ ref('slv_plan') }} p
left join base b on b.plan_id = p.plan_id
