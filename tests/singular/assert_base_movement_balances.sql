-- Subscriber base roll-forward must balance for every month/region/contract:
-- closing = opening + gross adds - churn +/- other movements (suspension etc. stay in base).
-- Flags groups where churn exceeds the opening base (impossible) or closing base is negative.
select *
from {{ ref('mart_churn_monthly') }}
where churned_lines > opening_base
   or closing_base < 0
