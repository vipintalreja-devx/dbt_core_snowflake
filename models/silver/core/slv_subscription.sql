{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'core'],
    transient=true
) }}
-- Current state of every line. Soft-deleted lines and lines owned by test customers
-- are kept but flagged; gold reporting filters on is_reportable.
with sub as (
    select * from {{ source('core', 'subscription') }}
),
acct as (
    select account_id, customer_id from {{ ref('slv_account') }}
),
cust as (
    select customer_id, is_test_customer from {{ ref('slv_customer') }}
)
select
    s.subscription_id,
    s.account_id,
    a.customer_id,
    s.plan_id,
    {{ normalize_msisdn('s.msisdn') }}                      as msisdn,
    upper(trim(s.status))                                   as status,
    {{ tz_to_utc('s.activated_at') }}                       as activated_at_utc,
    {{ utc_to_local(tz_to_utc('s.activated_at')) }}::date   as activation_date,
    s.deactivated_at                                        as deactivation_date,
    coalesce(s.phone_service, false)                        as has_phone_service,
    s.multiple_lines,
    s.internet_service,
    s.online_security,
    s.online_backup,
    s.device_protection,
    s.tech_support,
    s.streaming_tv,
    s.streaming_movies,
    s.monthly_charges::number(18, 2)                        as monthly_charges_php,
    s.total_charges::number(18, 2)                          as total_charges_php,
    s.tenure_months,
    s.home_cell_id,
    s.device_id,
    s.partner_brand_id,
    s.partner_brand_id is not null                          as is_mvno_line,
    s.lifetime_value_decfloat::number(38, 4)                as lifetime_value_php,
    coalesce(s.is_deleted, false)                           as is_deleted,
    coalesce(c.is_test_customer, false)                     as is_test,
    not coalesce(s.is_deleted, false)
        and not coalesce(c.is_test_customer, false)         as is_reportable
from sub s
left join acct a on a.account_id = s.account_id
left join cust c on c.customer_id = a.customer_id
