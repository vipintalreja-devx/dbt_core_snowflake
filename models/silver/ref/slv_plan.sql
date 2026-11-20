{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'ref'],
    transient=true
) }}
with plan as (
    select *
    from {{ source('ref', 'plan_catalog') }}
    qualify row_number() over (partition by plan_id order by effective_from desc) = 1
),
features as (
    select
        plan_id,
        listagg(feature_code, ',') within group (order by feature_code) as feature_list,
        count(*)                                                        as feature_count
    from {{ source('ref', 'plan_feature') }}
    where is_included
      and current_date between effective_from and coalesce(effective_to, '9999-12-31'::date)
    group by plan_id
)
select
    p.plan_id,
    p.plan_name,
    p.contract_type,
    decode(p.contract_type, 'Month-to-month', 1, 'One year', 12, 'Two year', 24) as commitment_months,
    iff(p.contract_type = 'Month-to-month', 'PREPAID_FLEX', 'POSTPAID')          as plan_category,
    p.internet_service,
    p.base_price::number(18, 2)                             as base_price_php,
    p.effective_from,
    p.effective_to,
    current_date between p.effective_from and coalesce(p.effective_to, '9999-12-31'::date) as is_sellable,
    p.attributes:speed_mbps::number                         as speed_mbps,
    p.attributes:data_cap_gb::number                        as data_cap_gb,
    coalesce(p.attributes:promo::boolean, false)            as is_promo,
    p.attributes:region_ids                                 as region_ids,
    coalesce(f.feature_list, '')                            as feature_list,
    coalesce(f.feature_count, 0)                            as feature_count
from plan p
left join features f on f.plan_id = p.plan_id
