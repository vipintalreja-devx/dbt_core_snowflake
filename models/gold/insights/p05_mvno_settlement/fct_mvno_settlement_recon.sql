{{ config(
    materialized='incremental',
    schema='gold',
    tags=['insights', 'p05', 'partner', 'finance'],
    incremental_strategy='delete+insert',
    unique_key='month_start',
    cluster_by=['month_start'],
    on_schema_change='append_new_columns'
) }}
-- P05 MVNO settlement reconciliation: what each partner should be settled for vs what the
-- settlement system recorded.
--   expected gross   = retail revenue of the partner's lines in the month (fct_mvno_line_monthly)
--   expected share   = expected gross x the brand's revenue-share %
--   wholesale charge = usage priced at the partner's wholesale rates
--   FX check         = settlement FX rate vs the BSP USD/PHP rate in force at month end (ASOF JOIN)
-- recon_status: MATCHED (|share variance| <= 1%), WITHIN_TOLERANCE (<= 5%), DISPUTE (> 5%),
--               NOT_SETTLED (month has no settlement row yet).
-- Grain   : month_start x partner_brand_id
-- Sources : fct_mvno_line_monthly, slv_partner_brand, slv_partner_settlement, slv_fx_rate
with expected as (
    select month_start, partner_brand_id, any_value(brand_name) as brand_name,
           count_if(is_in_base_eom)                                         as lines_eom,
           count_if(is_gross_add)                                           as gross_adds,
           count_if(is_churned)                                             as churned_lines,
           sum(retail_revenue_php)                                          as expected_gross_php,
           sum(wholesale_charge_php)                                        as wholesale_charge_php
    from {{ ref('fct_mvno_line_monthly') }}
    where {{ f2_month_window('month_start', 'month_start') }}
    group by 1, 2
),
settled as (
    select settlement_month, partner_brand_id, settlement_id, gross_revenue_php, share_php, fx_rate,
           settled_at_utc, settlement_status
    from {{ ref('slv_partner_settlement') }}
    qualify row_number() over (partition by partner_brand_id, settlement_month order by settled_at_utc desc nulls last, settlement_id desc) = 1
),
fx as (
    select valid_from, rate as bsp_usd_php
    from {{ ref('slv_fx_rate') }}
    where from_currency = 'USD' and to_currency = 'PHP'
),
joined as (
    select e.*, b.revenue_share_pct, s.settlement_id, s.gross_revenue_php, s.share_php, s.fx_rate,
           s.settled_at_utc, s.settlement_status, last_day(e.month_start) as month_end
    from expected e
    join {{ ref('slv_partner_brand') }} b on b.partner_brand_id = e.partner_brand_id
    left join settled s on s.partner_brand_id = e.partner_brand_id and s.settlement_month = e.month_start
),
with_fx as (
    select j.*, f.bsp_usd_php
    from joined j
    asof join fx f match_condition (j.month_end >= f.valid_from)
)
select
    month_start, partner_brand_id, brand_name, lines_eom, gross_adds, churned_lines,
    revenue_share_pct,
    round(expected_gross_php, 2)                                            as expected_gross_php,
    round(expected_gross_php * revenue_share_pct / 100, 2)                  as expected_share_php,
    round(wholesale_charge_php, 2)                                          as wholesale_charge_php,
    settlement_id,
    gross_revenue_php                                                       as settled_gross_php,
    share_php                                                               as settled_share_php,
    round(gross_revenue_php - expected_gross_php, 2)                        as gross_variance_php,
    round(share_php - expected_gross_php * revenue_share_pct / 100, 2)      as share_variance_php,
    round(div0(share_php - expected_gross_php * revenue_share_pct / 100,
               expected_gross_php * revenue_share_pct / 100) * 100, 2)      as share_variance_pct,
    fx_rate                                                                 as settlement_fx_rate,
    bsp_usd_php,
    round(fx_rate - bsp_usd_php, 6)                                         as fx_rate_diff,
    settlement_status,
    settled_at_utc,
    datediff('day', month_end, settled_at_utc::date)                        as days_to_settle,
    case when settlement_id is null then 'NOT_SETTLED'
         when abs(div0(share_php - expected_gross_php * revenue_share_pct / 100, expected_gross_php * revenue_share_pct / 100)) <= 0.01 then 'MATCHED'
         when abs(div0(share_php - expected_gross_php * revenue_share_pct / 100, expected_gross_php * revenue_share_pct / 100)) <= 0.05 then 'WITHIN_TOLERANCE'
         else 'DISPUTE' end                                                 as recon_status,
    {{ dbt_loaded_at() }}                                                   as _dbt_loaded_at
from with_fx
