-- Gold monthly revenue must equal silver earned revenue for every month (no rows lost/duplicated
-- by the line x month spine). Returns months that break by more than 1 PHP.
with g as (
    select month_start, sum(total_revenue_php) as gold_php
    from {{ ref('fct_subscriber_monthly') }} group by 1
),
s as (
    select date_trunc('month', business_date)::date as month_start, sum(revenue_php) as silver_php
    from {{ ref('slv_daily_revenue') }} group by 1
)
select s.month_start, s.silver_php, g.gold_php
from s left join g on g.month_start = s.month_start
where abs(s.silver_php - zeroifnull(g.gold_php)) > 1
