{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dims', 'dimension'],
    transient=false,
    copy_grants=true,
    post_hook="alter table {{ this }} set comment = 'Gold dimension - dim_campaign (rebuilt {{ run_started_at.strftime(\'%Y-%m-%d %H:%M\') }} UTC)'"
) }}
-- Materialization: table (current-state/small reference): dimension (small, current state; history comes from the SCD2 snapshots).
select campaign_id, campaign_name, channel, start_date, end_date, budget_php, is_active,
       datediff('day', start_date, end_date) + 1 as duration_days
from {{ ref('slv_campaign') }}
