{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'core'],
    transient=true
) }}
select
    contract_id,
    subscription_id,
    contract_type,
    decode(contract_type, 'Month-to-month', 1, 'One year', 12, 'Two year', 24) as commitment_months,
    start_date,
    end_date,
    coalesce(auto_renew, false)                             as is_auto_renew,
    terminated_on,
    nullif(trim(termination_reason), '')                    as termination_reason,
    terminated_on is not null                               as is_terminated
from {{ source('core', 'contract') }}
qualify row_number() over (partition by contract_id order by start_date desc) = 1
