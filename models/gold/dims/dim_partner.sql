{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dims', 'dimension'],
    transient=false,
    copy_grants=true,
    post_hook="alter table {{ this }} set comment = 'Gold dimension - dim_partner (rebuilt {{ run_started_at.strftime(\'%Y-%m-%d %H:%M\') }} UTC)'"
) }}
-- Materialization: table (current-state/small reference): dimension (small, current state; history comes from the SCD2 snapshots).
with lines as (
    select partner_brand_id, count(*) as mapped_lines from {{ ref('slv_partner_subscriber_map') }} group by 1
)
select b.*, coalesce(l.mapped_lines, 0) as mapped_lines
from {{ ref('slv_partner_brand') }} b
left join lines l on l.partner_brand_id = b.partner_brand_id
