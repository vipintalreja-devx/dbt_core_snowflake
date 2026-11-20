{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'core'],
    transient=true
) }}
-- Status intervals per line (SCD2 from the BSS). Bounds are Manila local NTZ in bronze.
-- valid_to_local is exclusive; open interval => '9999-12-31'.
-- Bronze contains zero-length intervals (VALID_FROM = VALID_TO, e.g. ACTIVE->SUSPENDED in the
-- same instant). They carry no time in the status and would double-count the line, so are dropped.
select
    subscription_id,
    upper(trim(status))                                     as status,
    valid_from::timestamp_ntz                               as valid_from_local,
    coalesce(valid_to::timestamp_ntz, '9999-12-31'::timestamp_ntz) as valid_to_local,
    valid_from::date                                        as valid_from_date,
    coalesce(valid_to::date, '9999-12-31'::date)            as valid_to_date,
    valid_to is null                                        as is_current,
    upper(trim(status)) in ('ACTIVE', 'SUSPENDED')          as is_in_base,
    row_number() over (partition by subscription_id order by valid_from) as status_seq
from {{ source('core', 'subscription_status_history') }}
where valid_to is null or valid_to > valid_from
