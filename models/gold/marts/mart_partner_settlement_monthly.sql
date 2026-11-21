{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'marts', 'kpi'],
    transient=false,
    copy_grants=true
) }}
-- Materialization: table (current-state/small reference): tiny reference (60 settlement rows).
-- MVNO wholesale settlement check: expected revenue share (from earned revenue of the
-- partner's lines x contractual share %) vs. the settlement actually booked.
with earned as (
    select pm.partner_brand_id, m.month_start,
           count(distinct iff(m.is_in_base_eom, m.subscription_id, null)) as active_lines,
           sum(m.total_revenue_php)                         as gross_revenue_php,
           sum(m.data_mb) / 1024                            as data_gb,
           sum(m.voice_min)                                 as voice_min
    from {{ ref('fct_subscriber_monthly') }} m
    join {{ ref('slv_partner_subscriber_map') }} pm on pm.subscription_id = m.subscription_id
    group by 1, 2
)
select
    e.month_start,
    b.partner_brand_id,
    b.brand_name,
    b.revenue_share_pct,
    e.active_lines,
    round(e.gross_revenue_php, 2)                           as gross_revenue_php,
    round(e.data_gb, 2)                                     as data_gb,
    round(e.voice_min, 1)                                   as voice_min,
    round(e.gross_revenue_php * b.revenue_share_pct / 100, 2) as expected_share_php,
    s.share_php                                             as booked_share_php,
    s.settlement_status,
    round(zeroifnull(s.share_php) - e.gross_revenue_php * b.revenue_share_pct / 100, 2) as share_variance_php
from earned e
join {{ ref('slv_partner_brand') }} b on b.partner_brand_id = e.partner_brand_id
left join {{ ref('slv_partner_settlement') }} s
       on s.partner_brand_id = e.partner_brand_id and s.settlement_month = e.month_start
