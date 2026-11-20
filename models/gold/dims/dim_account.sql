{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dims', 'dimension'],
    transient=false,
    copy_grants=true,
    post_hook="alter table {{ this }} set comment = 'Gold dimension - dim_account (rebuilt {{ run_started_at.strftime(\'%Y-%m-%d %H:%M\') }} UTC)'"
) }}
with lines as (
    select account_id,
           count(*)                                         as total_lines,
           count_if(status in ('ACTIVE', 'SUSPENDED'))      as active_lines
    from {{ ref('slv_subscription') }}
    where not is_deleted
    group by 1
)
select
    a.account_id, a.customer_id, a.payment_method, a.is_paperless_billing, a.billing_cycle_day,
    a.created_date, coalesce(l.total_lines, 0) as total_lines, coalesce(l.active_lines, 0) as active_lines
from {{ ref('slv_account') }} a
left join lines l on l.account_id = a.account_id
