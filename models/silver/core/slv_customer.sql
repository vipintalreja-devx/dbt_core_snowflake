{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'core'],
    transient=true
) }}
-- Party master, cleaned. PII (name, DOB, KYC document number) stays in silver only;
-- gold dim_customer exposes derived, non-identifying attributes.
select
    customer_id,
    customer_code,
    nullif(upper(trim(gender)), '')                         as gender,
    coalesce(is_senior, false)                              as is_senior,
    coalesce(has_partner, false)                            as has_partner,
    coalesce(has_dependents, false)                         as has_dependents,
    full_name,
    date_of_birth,
    nullif(trim(preferred_language), '')                    as preferred_language,
    kyc_payload:doc_type::string                            as kyc_doc_type,
    try_to_date(kyc_payload:verified_on::string)            as kyc_verified_on,
    kyc_payload:verified_by::string                         as kyc_verified_by,
    kyc_payload:address_proof::boolean                      as kyc_has_address_proof,
    kyc_payload:liveness_score::float                       as kyc_liveness_score,
    coalesce(array_size(consent_flags), 0)                  as consent_flag_count,
    {{ local_ntz_to_utc('created_at') }}                    as created_at_utc,
    coalesce(is_test_customer, false)                       as is_test_customer
from {{ source('core', 'customer') }}
