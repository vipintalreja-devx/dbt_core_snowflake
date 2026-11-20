{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dims', 'dimension'],
    transient=false,
    copy_grants=true,
    post_hook="alter table {{ this }} set comment = 'Gold dimension - dim_customer (rebuilt {{ run_started_at.strftime(\'%Y-%m-%d %H:%M\') }} UTC)'"
) }}
-- Analyst-safe customer dimension: no name, DOB or KYC document data - only derived bands.
with consent as (
    select
        customer_id,
        max(iff(consent_type = 'MARKETING' and is_granted, 1, 0)) = 1    as has_marketing_consent,
        max(iff(consent_type = 'DATA_SHARING' and is_granted, 1, 0)) = 1 as has_data_sharing_consent
    from {{ ref('slv_customer_consent') }}
    group by 1
)
select
    c.customer_id,
    c.customer_code,
    c.gender,
    c.is_senior,
    c.has_partner,
    c.has_dependents,
    case
        when c.date_of_birth is null then 'UNKNOWN'
        when datediff('year', c.date_of_birth, current_date) < 25 then '18-24'
        when datediff('year', c.date_of_birth, current_date) < 35 then '25-34'
        when datediff('year', c.date_of_birth, current_date) < 45 then '35-44'
        when datediff('year', c.date_of_birth, current_date) < 55 then '45-54'
        when datediff('year', c.date_of_birth, current_date) < 65 then '55-64'
        else '65+'
    end                                                     as age_band,
    c.preferred_language,
    c.kyc_doc_type,
    c.kyc_verified_on is not null                           as is_kyc_verified,
    coalesce(cn.has_marketing_consent, false)               as has_marketing_consent,
    coalesce(cn.has_data_sharing_consent, false)            as has_data_sharing_consent,
    c.created_at_utc,
    c.is_test_customer
from {{ ref('slv_customer') }} c
left join consent cn on cn.customer_id = c.customer_id
