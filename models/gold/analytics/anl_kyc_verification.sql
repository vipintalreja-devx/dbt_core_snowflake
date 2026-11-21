{{ config(
    materialized='table',
    tags=['gold', 'analytics', 'compliance', 'kyc', 'm24']
) }}
-- Materialization: table (current-state/small reference): current-state profile of the subscriber/device base.
-- SIM-registration / KYC quality (Philippine SIM Registration Act): verification channel, document
-- type, liveness band, address proof and consent coverage - no personal fields kept.
-- Ported from x_m24_kyc_verification_snapshot (PIPE_X.SP_X_M24_KYC_VERIFICATION_SNAPSHOT).
-- Grain: doc_type x verified_by x liveness_band x island_group.
-- Sources: slv_customer, slv_customer_consent, dim_account, int_subscriber_profile.
with cust as (
    select
        c.customer_id,
        coalesce(c.kyc_doc_type, 'NONE')                    as doc_type,
        coalesce(c.kyc_verified_by, 'NONE')                 as verified_by,
        case when c.kyc_liveness_score >= 0.9 then 'HIGH'
             when c.kyc_liveness_score >= 0.7 then 'MEDIUM'
             when c.kyc_liveness_score is null then 'MISSING'
             else 'LOW' end                                 as liveness_band,
        c.kyc_has_address_proof,
        c.kyc_verified_on,
        c.created_at_utc
    from {{ ref('slv_customer') }} c
    where not c.is_test_customer
),
consent as (
    select customer_id,
           boolor_agg(consent_type = 'MARKETING' and is_granted)    as marketing_consent,
           boolor_agg(consent_type = 'DATA_SHARING' and is_granted) as data_sharing_consent
    from {{ ref('slv_customer_consent') }}
    group by 1
),
geo as (
    select a.customer_id,
           sum(a.active_lines)                              as active_lines,
           any_value(p.island_group)                        as island_group
    from {{ ref('dim_account') }} a
    left join {{ ref('int_subscriber_profile') }} p on p.account_id = a.account_id
    group by 1
)
select
    c.doc_type,
    c.verified_by,
    c.liveness_band,
    coalesce(g.island_group, 'UNKNOWN')                     as island_group,
    count(*)                                                as customers,
    sum(zeroifnull(g.active_lines))                         as active_lines,
    count_if(c.kyc_has_address_proof)                       as with_address_proof,
    count_if(c.kyc_verified_on is null)                     as missing_or_bad_verified_date,
    count_if(c.kyc_verified_on > c.created_at_utc::date + 30) as verified_after_30_days,
    round(median(datediff('day', c.created_at_utc::date, c.kyc_verified_on)), 1) as median_days_to_verify,
    count_if(cn.marketing_consent)                          as with_marketing_consent,
    count_if(cn.data_sharing_consent)                       as with_data_sharing_consent,
    count_if(c.doc_type = 'NONE' and zeroifnull(g.active_lines) > 0) as unregistered_with_active_lines,
    round(ratio_to_report(count(*)) over (partition by coalesce(g.island_group, 'UNKNOWN')), 4) as share_of_island_customers,
    current_timestamp()                                     as loaded_at
from cust c
left join consent cn on cn.customer_id = c.customer_id
left join geo g      on g.customer_id = c.customer_id
group by 1, 2, 3, 4
