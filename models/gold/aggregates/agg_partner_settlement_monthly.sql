{{ config(
    materialized='table',
    cluster_by=['settlement_month'],
    tags=['gold', 'aggregates', 'partner', 'finance', 'e27']
) }}
-- MVNO partner settlements per month x brand x status: effective vs contractual revenue share,
-- USD value at the month-end rate and settlement lead time. Full history.
-- Ported from x_e27_partner_settlement_monthly (PIPE_X.SP_X_E27_PARTNER_SETTLEMENT_MONTHLY).
-- Grain   : settlement_month x brand_name x settlement_status
-- Sources : slv_partner_settlement, slv_partner_brand, slv_fx_rate (ASOF), slv_partner_subscriber_map
with lines as (
    select partner_brand_id, count(*) as mapped_lines from {{ ref('slv_partner_subscriber_map') }} group by 1
),
usd as (
    select valid_from, rate from {{ ref('slv_fx_rate') }} where from_currency = 'USD' and to_currency = 'PHP'
),
s as (
    select
        st.settlement_month, st.settlement_id, st.partner_brand_id, st.gross_revenue_php, st.share_php,
        st.fx_rate, st.settled_at_utc, st.settlement_status,
        b.brand_name, b.operator_type, b.revenue_share_pct,
        zeroifnull(l.mapped_lines)                          as mapped_lines,
        last_day(st.settlement_month)                       as month_end
    from {{ ref('slv_partner_settlement') }} st
    join {{ ref('slv_partner_brand') }} b on b.partner_brand_id = st.partner_brand_id
    left join lines l                    on l.partner_brand_id = st.partner_brand_id
),
s_fx as (
    select s.*, u.rate as usd_php_rate_month_end
    from s
    asof join usd u match_condition (s.month_end >= u.valid_from)
)
select
    settlement_month, brand_name, settlement_status,
    any_value(operator_type)                                as operator_type,
    any_value(revenue_share_pct)                            as contractual_share_pct,
    count(*)                                                as settlements,
    sum(gross_revenue_php)                                  as gross_revenue_php,
    sum(share_php)                                          as share_php,
    round(div0(sum(share_php), sum(gross_revenue_php)) * 100, 3) as effective_share_pct,
    round(div0(sum(share_php), sum(gross_revenue_php)) * 100 - any_value(revenue_share_pct), 3) as share_pct_deviation,
    round(sum(gross_revenue_php) * any_value(revenue_share_pct) / 100 - sum(share_php), 2) as share_shortfall_php,
    round(div0(sum(share_php), any_value(usd_php_rate_month_end)), 2) as share_usd_at_month_end,
    round(avg(fx_rate), 6)                                  as avg_settlement_fx_rate,
    round(avg(datediff('day', month_end, {{ utc_to_local('settled_at_utc') }}::date)), 1) as avg_days_to_settle,
    round(div0(sum(gross_revenue_php), any_value(mapped_lines)), 2) as gross_revenue_per_mapped_line_php,
    round(div0(sum(share_php) - lag(sum(share_php)) over (partition by brand_name, settlement_status order by settlement_month),
               lag(sum(share_php)) over (partition by brand_name, settlement_status order by settlement_month)) * 100, 2) as share_mom_pct,
    current_timestamp()                                     as loaded_at
from s_fx
group by 1, 2, 3
