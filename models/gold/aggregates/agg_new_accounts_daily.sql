{{ config(
    materialized='table',
    cluster_by=['created_date'],
    tags=['gold', 'aggregates', 'core', 'acquisition', 'e11']
) }}
-- Materialization: table (current-state/small reference): reads the account master (600k rows, not a CDC event feed).
-- New billing accounts per creation day x payment method x paperless flag, with customer and line mix.
-- Ported from x_e11_new_accounts_daily (PIPE_X.SP_X_E11_NEW_ACCOUNTS_DAILY). Account creation is
-- sparse inside the dev window, so this aggregate covers full history (small table).
-- Grain   : created_date x payment_method x is_paperless_billing
-- Sources : slv_account, dim_customer, slv_subscription, int_subscriber_profile
with lines as (
    select account_id,
           count(*)                                         as lines,
           count_if(status in ('ACTIVE', 'SUSPENDED'))      as active_lines,
           count_if(status = 'CHURNED')                     as churned_lines,
           sum(monthly_charges_php)                         as monthly_charges_php
    from {{ ref('slv_subscription') }}
    group by 1
),
acct as (
    select
        a.account_id, a.created_date, coalesce(a.payment_method, 'UNKNOWN') as payment_method, a.is_paperless_billing,
        c.age_band, c.is_senior, c.is_test_customer, c.is_kyc_verified,
        coalesce(l.lines, 0) as lines, coalesce(l.active_lines, 0) as active_lines,
        coalesce(l.churned_lines, 0) as churned_lines, coalesce(l.monthly_charges_php, 0) as monthly_charges_php,
        p.island_group
    from {{ ref('slv_account') }} a
    left join {{ ref('dim_customer') }} c on c.customer_id = a.customer_id
    left join lines l                    on l.account_id = a.account_id
    left join (select account_id, max_by(island_group, monthly_charges_php) as island_group
               from {{ ref('int_subscriber_profile') }} group by 1) p on p.account_id = a.account_id
)
select
    created_date, payment_method, is_paperless_billing,
    count(*)                                                as accounts,
    count_if(is_senior)                                     as senior_accounts,
    count_if(is_test_customer)                              as test_accounts,
    count_if(is_kyc_verified)                               as kyc_verified_accounts,
    sum(lines)                                              as lines_opened,
    sum(active_lines)                                       as lines_still_active,
    sum(churned_lines)                                      as lines_churned_since,
    round(div0(sum(active_lines), sum(lines)) * 100, 2)     as cohort_line_survival_pct,
    round(avg(monthly_charges_php), 2)                      as avg_account_monthly_charges_php,
    mode(age_band)                                          as dominant_age_band,
    array_agg(distinct island_group) within group (order by island_group) as island_groups,
    sum(count(*)) over (partition by payment_method, is_paperless_billing order by created_date
                        rows between unbounded preceding and current row) as cumulative_accounts,
    current_timestamp()                                     as loaded_at
from acct
where created_date is not null
group by 1, 2, 3
