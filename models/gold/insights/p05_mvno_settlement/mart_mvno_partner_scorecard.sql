{{ config(
    materialized='table',
    schema='gold',
    tags=['insights', 'p05', 'partner', 'mart'],
    transient=false,
    copy_grants=true
) }}
-- P05 MVNO settlement: partner scorecard (current state, rebuilt daily -- 5 rows).
-- Purpose : one view of each MVNO partner over the last 6 complete months: base and churn,
--           revenue trend (REGR_SLOPE per month), ARPU, wholesale margin to the host network,
--           reconciliation health (months in dispute / not settled) and settlement timeliness.
-- Grain   : partner_brand_id
-- Sources : fct_mvno_settlement_recon, slv_partner_brand
with recent as (
    select r.*,
           dense_rank() over (order by month_start desc)                    as month_recency
    from {{ ref('fct_mvno_settlement_recon') }} r
    -- the latest month is still in progress: score complete months only
    where month_start < (select max(month_start) from {{ ref('fct_mvno_settlement_recon') }})
),
six as (
    select * from recent where month_recency <= 6
)
select
    b.partner_brand_id, b.brand_name, b.operator_type, b.is_active, b.revenue_share_pct,
    max(month_start)                                                        as latest_month,
    max_by(lines_eom, month_start)                                          as lines_latest,
    sum(churned_lines)                                                      as churned_lines_6m,
    round(div0(sum(churned_lines), avg(lines_eom)) * 100, 2)                as churn_rate_6m_pct,
    round(sum(expected_gross_php), 2)                                       as expected_gross_6m_php,
    round(div0(sum(expected_gross_php), sum(lines_eom)), 2)                 as arpu_php,
    round(regr_slope(expected_gross_php, datediff('month', '2000-01-01'::date, month_start)), 2) as revenue_trend_php_per_month,
    round(sum(wholesale_charge_php), 2)                                     as wholesale_charge_6m_php,
    round(sum(expected_share_php) - sum(wholesale_charge_php), 2)           as partner_net_after_wholesale_php,
    count_if(recon_status = 'MATCHED')                                      as months_matched,
    count_if(recon_status = 'WITHIN_TOLERANCE')                             as months_within_tolerance,
    count_if(recon_status = 'DISPUTE')                                      as months_in_dispute,
    count_if(recon_status = 'NOT_SETTLED')                                  as months_not_settled,
    round(avg(abs(share_variance_pct)), 2)                                  as avg_abs_share_variance_pct,
    round(avg(days_to_settle), 1)                                           as avg_days_to_settle,
    count_if(settlement_status = 'PENDING')                                 as pending_settlements,
    case when count_if(recon_status = 'DISPUTE') >= 2 then 'ESCALATE'
         when count_if(recon_status in ('DISPUTE', 'NOT_SETTLED')) >= 1 then 'WATCH'
         else 'OK' end                                                      as partner_status
from {{ ref('slv_partner_brand') }} b
left join six s on s.partner_brand_id = b.partner_brand_id
group by 1, 2, 3, 4, 5
