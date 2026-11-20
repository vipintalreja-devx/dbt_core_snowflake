{{ config(
    materialized='view',
    schema='silver',
    tags=['silver', 'intermediate']
) }}
-- Conformed, current profile of every line: the single place where home region,
-- plan, device, partner brand, segment and churn band are resolved.
-- (Each stored procedure re-implemented these joins with slightly different rules.)
with seg as (
    select subscription_id, segment_code
    from {{ ref('slv_segment_membership') }}
    where is_current
    qualify row_number() over (partition by subscription_id order by valid_from desc) = 1
),
ctr as (
    select *
    from {{ ref('slv_contract') }}
    qualify row_number() over (partition by subscription_id order by start_date desc) = 1
)
select
    s.subscription_id,
    s.account_id,
    s.customer_id,
    s.msisdn,
    s.status,
    s.activation_date,
    s.deactivation_date,
    s.tenure_months,
    s.monthly_charges_php,
    s.plan_id,
    p.plan_name,
    coalesce(ctr.contract_type, p.contract_type)            as contract_type,
    coalesce(ctr.commitment_months, p.commitment_months)    as commitment_months,
    p.plan_category,
    s.internet_service,
    s.home_cell_id,
    c.region_id                                             as home_region_id,
    coalesce(c.region_name, 'UNKNOWN')                      as home_region_name,
    coalesce(c.island_group, 'UNKNOWN')                     as island_group,
    s.device_id,
    d.manufacturer                                          as device_manufacturer,
    d.supports_5g                                           as device_supports_5g,
    s.partner_brand_id,
    coalesce(pb.brand_name, 'OWN BRAND')                    as brand_name,
    s.is_mvno_line,
    ctr.contract_id,
    ctr.end_date                                            as contract_end_date,
    ctr.is_auto_renew                                       as contract_auto_renew,
    seg.segment_code,
    cs.churn_probability,
    cs.churn_band,
    cs.score_date                                           as churn_score_date,
    s.is_deleted,
    s.is_test,
    s.is_reportable
from {{ ref('slv_subscription') }} s
left join {{ ref('slv_plan') }}          p   on p.plan_id = s.plan_id
left join {{ ref('slv_cell_site') }}     c   on c.cell_id = s.home_cell_id
left join {{ ref('slv_device') }}        d   on d.device_id = s.device_id
left join {{ ref('slv_partner_brand') }} pb  on pb.partner_brand_id = s.partner_brand_id
left join ctr                                on ctr.subscription_id = s.subscription_id
left join seg                                on seg.subscription_id = s.subscription_id
left join {{ ref('slv_churn_score') }}   cs  on cs.subscription_id = s.subscription_id
