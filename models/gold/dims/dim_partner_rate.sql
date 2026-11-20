{{ config(
    materialized='table',
    schema='gold',
    cluster_by=['partner_brand_id'],
    tags=['gold', 'dims', 'partner', 'g4'],
    transient=false
) }}
-- In-force MVNO wholesale rate per brand / service / unit, with the previous rate, the USD
-- equivalent at the latest FX rate, whether the rate is confirmed by the latest landing file,
-- and the brand's last settlement.
-- Ported from telco_v2_core g4_dim_partner_rate (PIPE.SP_GOLD_PARTNER_RATE).
-- Grain: partner_brand_id x service_type x unit (current rate).
-- Sources (6): slv_partner_rate_card, slv_partner_brand, slv_fx_rate, slv_partner_settlement,
-- slv_landing_partner_rate, dim_partner.
with card as (
    select rc.*,
           lag(rc.rate_php) over (partition by rc.partner_brand_id, rc.service_type, rc.unit order by rc.effective_from) as prev_rate_php,
           count(*) over (partition by rc.partner_brand_id, rc.service_type, rc.unit) as rate_versions
    from {{ ref('slv_partner_rate_card') }} rc
    where rc.effective_from <= current_date
),
in_force as (
    select * from card
    qualify row_number() over (partition by partner_brand_id, service_type, unit order by effective_from desc) = 1
),
usd as (
    select rate as usd_php, valid_from as fx_date
    from {{ ref('slv_fx_rate') }}
    where from_currency = 'USD' and to_currency = 'PHP'
    qualify row_number() over (order by valid_from desc) = 1
),
settled as (
    select ps.partner_brand_id, ps.settlement_month, ps.share_php, ps.settlement_status
    from {{ ref('slv_partner_settlement') }} ps
    qualify row_number() over (partition by ps.partner_brand_id order by ps.settlement_month desc) = 1
),
file_rates as (
    select partner_brand_id, service_type, unit, rate_php, max(loaded_at_utc) as file_loaded_at_utc,
           any_value(file_name) as file_name
    from {{ ref('slv_landing_partner_rate') }}
    where is_valid
    group by 1, 2, 3, 4
)
select
    {{ dbt_utils.generate_surrogate_key(['f.partner_brand_id', 'f.service_type', 'f.unit']) }} as partner_rate_key,
    f.partner_brand_id,
    f.service_type,
    f.unit,
    b.brand_name,
    b.operator_type,
    b.revenue_share_pct,
    f.rate_php,
    f.effective_from                                            as rate_version_from,
    f.rate_versions,
    round(div0(f.rate_php, u.usd_php), 8)                       as rate_usd,
    u.fx_date,
    f.prev_rate_php,
    round(div0(f.rate_php - f.prev_rate_php, f.prev_rate_php) * 100, 2) as rate_change_pct,
    fr.partner_brand_id is not null                             as is_in_latest_file,
    fr.file_name                                                as confirming_file,
    st.settlement_month                                         as last_settled_month,
    st.share_php                                                as last_share_php,
    st.settlement_status                                        as last_settlement_status,
    ratio_to_report(st.share_php) over (partition by f.service_type, f.unit) as brand_settlement_share,
    dp.mapped_lines                                             as brand_mapped_lines
from in_force f
join {{ ref('slv_partner_brand') }} b on b.partner_brand_id = f.partner_brand_id
cross join usd u
left join settled st   on st.partner_brand_id = f.partner_brand_id
left join file_rates fr
       on fr.partner_brand_id = f.partner_brand_id and fr.service_type = f.service_type
      and fr.unit = f.unit and fr.rate_php = f.rate_php
left join {{ ref('dim_partner') }} dp on dp.partner_brand_id = f.partner_brand_id
