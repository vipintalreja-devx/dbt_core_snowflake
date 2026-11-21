{{ config(
    materialized='table',
    tags=['gold', 'analytics', 'customers', 'demographics', 'm37']
) }}
-- Materialization: table (current-state/small reference): current-state profile of the subscriber/device base.
-- Customer base demographics per age band, gender, language and island group: customers, active
-- lines, lines per customer, spend, data usage, churn risk and digital adoption.
-- Ported from x_m37_customer_demographics (PIPE_X.SP_X_M37_CUSTOMER_DEMOGRAPHICS).
-- Grain: age_band x gender x preferred_language x island_group.
-- Sources: dim_customer, dim_account, int_subscriber_profile, fct_subscriber_monthly.
with last_month as (
    select max(month_start) as month_start from {{ ref('fct_subscriber_monthly') }}
),
lines as (
    select
        p.customer_id,
        any_value(p.island_group)                           as island_group,
        count(*)                                            as active_lines,
        sum(p.monthly_charges_php)                          as monthly_charges_php,
        avg(p.churn_probability)                            as avg_churn_probability,
        count_if(p.device_supports_5g)                      as lines_5g_device,
        count_if(p.is_mvno_line)                            as mvno_lines,
        sum(m.total_revenue_php)                            as revenue_last_month_php,
        sum(m.data_mb)                                      as data_mb_last_month
    from {{ ref('int_subscriber_profile') }} p
    left join {{ ref('fct_subscriber_monthly') }} m
           on m.subscription_id = p.subscription_id
          and m.month_start = (select month_start from last_month)
    where p.is_reportable and p.status = 'ACTIVE'
    group by 1
),
acct as (
    select customer_id, boolor_agg(is_paperless_billing) as any_paperless, count(*) as accounts
    from {{ ref('dim_account') }} group by 1
)
select
    c.age_band,
    coalesce(c.gender, 'UNKNOWN')                           as gender,
    coalesce(c.preferred_language, 'UNKNOWN')               as preferred_language,
    coalesce(l.island_group, 'NO_ACTIVE_LINE')              as island_group,
    count(*)                                                as customers,
    count_if(l.customer_id is not null)                     as customers_with_active_line,
    sum(zeroifnull(l.active_lines))                         as active_lines,
    round(div0(sum(zeroifnull(l.active_lines)), count_if(l.customer_id is not null)), 2) as lines_per_active_customer,
    round(div0(sum(l.monthly_charges_php), sum(l.active_lines)), 2) as avg_monthly_charges_php,
    round(div0(sum(l.revenue_last_month_php), count_if(l.customer_id is not null)), 2) as revenue_per_customer_php,
    round(div0(sum(l.data_mb_last_month), sum(l.active_lines)) / 1024, 3) as data_gb_per_line,
    round(avg(l.avg_churn_probability), 4)                  as avg_churn_probability,
    round(div0(sum(l.lines_5g_device), sum(l.active_lines)), 4) as share_5g_devices,
    count_if(c.is_senior)                                   as senior_customers,
    count_if(a.any_paperless)                               as paperless_customers,
    count_if(c.has_marketing_consent)                       as marketing_consent_customers,
    round(ratio_to_report(count(*)) over (), 5)             as share_of_customers,
    current_timestamp()                                     as loaded_at
from {{ ref('dim_customer') }} c
left join lines l on l.customer_id = c.customer_id
left join acct a  on a.customer_id = c.customer_id
where not c.is_test_customer
group by 1, 2, 3, 4
