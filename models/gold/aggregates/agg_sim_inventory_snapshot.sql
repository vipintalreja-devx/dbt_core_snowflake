{{ config(
    materialized='table',
    tags=['gold', 'aggregates', 'core', 'inventory', 'snapshot', 'e12']
) }}
-- Current SIM stock per type x status x region of the line using the SIM, with stock ageing.
-- Ported from x_e12_sim_inventory_snapshot (PIPE_X.SP_X_E12_SIM_INVENTORY_SNAPSHOT).
-- Grain   : sim_type x sim_status x region_name   (current state)
-- Sources : slv_sim_inventory, slv_subscription, int_subscriber_profile, dim_region
with sims as (
    select
        s.iccid, s.sim_type, s.sim_status, s.received_on, s.activated_at_utc,
        datediff('day', s.received_on, coalesce({{ utc_to_local('s.activated_at_utc') }}::date, current_date)) as days_in_stock,
        sub.subscription_id,
        sub.status                                          as line_status,
        coalesce(p.home_region_name, iff(sub.subscription_id is null, 'WAREHOUSE', 'UNKNOWN')) as region_name
    from {{ ref('slv_sim_inventory') }} s
    left join {{ ref('slv_subscription') }} sub        on sub.msisdn = s.msisdn and s.msisdn is not null
    left join {{ ref('int_subscriber_profile') }} p     on p.subscription_id = sub.subscription_id
)
select
    current_date                                            as snapshot_date,
    sim_type, sim_status, region_name,
    count(*)                                                as sims,
    count(activated_at_utc)                                 as activated,
    count(subscription_id)                                  as linked_to_line,
    count_if(line_status in ('ACTIVE', 'SUSPENDED'))        as linked_to_active_line,
    count_if(sim_status = 'ACTIVE' and subscription_id is null)   as active_sim_without_line,
    count_if(sim_status = 'IN_STOCK' and days_in_stock > 180)     as stale_stock_180d,
    median(iff(sim_status = 'IN_STOCK', days_in_stock, null))     as median_days_in_stock,
    approx_percentile(iff(activated_at_utc is not null, days_in_stock, null), 0.5) as median_days_to_activation,
    min(received_on)                                        as oldest_received_on,
    round(ratio_to_report(count(*)) over (partition by sim_type), 6) as share_of_sim_type,
    current_timestamp()                                     as loaded_at
from sims
group by 1, 2, 3, 4
