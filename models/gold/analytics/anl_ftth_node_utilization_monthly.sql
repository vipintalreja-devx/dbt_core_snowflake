{{ config(
    materialized='table',
    cluster_by=['month_end'],
    tags=['gold', 'analytics', 'network', 'ftth', 'capacity', 'm19']
) }}
-- FTTH capacity planning at each month end: active fibre services vs. port capacity per SPLITTER,
-- CABINET and OLT (hierarchy walked with a recursive CTE), with headroom and exhaustion flags.
-- Ported from x_m19_ftth_node_utilization (PIPE_X.SP_X_M19_FTTH_NODE_UTILIZATION) - one row per
-- node per month for the whole window instead of a single snapshot date.
-- Grain: month_end x node_id (node_type in SPLITTER / CABINET / OLT).
-- Sources: source network.ftth_topology, slv_service_instance, slv_region, dim_date.
with recursive tree (node_id, ancestor_id, ancestor_type, depth) as (
    -- every LINE node climbs to each of its ancestors
    select node_id, parent_node_id, cast(null as varchar), 1
    from {{ source('network', 'ftth_topology') }}
    where node_type = 'LINE'
    union all
    select t.node_id, p.parent_node_id, p.node_type, t.depth + 1
    from tree t
    join {{ source('network', 'ftth_topology') }} p on p.node_id = t.ancestor_id
    where t.depth < 6
),
ancestors as (
    -- (line, ancestor) pairs for planning levels
    select t.node_id as line_node_id, a.node_id as ancestor_node_id, a.node_type as ancestor_type
    from tree t
    join {{ source('network', 'ftth_topology') }} a on a.node_id = t.ancestor_id
    where a.node_type in ('SPLITTER', 'CABINET', 'OLT')
),
month_ends as (
    select distinct month_end
    from {{ ref('dim_date') }}
    where date_day between '{{ var("history_start_date") }}'::date
                       and (select max(provisioned_at_utc)::date from {{ ref('slv_service_instance') }})
),
active as (
    select me.month_end, si.ftth_node_id as line_node_id
    from month_ends me
    join {{ ref('slv_service_instance') }} si
      on si.ftth_node_id is not null
     and si.provisioned_at_utc < dateadd('day', 1, me.month_end)
     and (si.decommissioned_at_utc is null or si.decommissioned_at_utc >= dateadd('day', 1, me.month_end))
),
used as (
    select a.month_end, an.ancestor_node_id as node_id, an.ancestor_type as node_type, count(*) as active_services
    from active a
    join ancestors an on an.line_node_id = a.line_node_id
    group by 1, 2, 3
),
-- leaf capacity below each node = number of LINE nodes (homes passed)
passed as (
    select ancestor_node_id as node_id, count(*) as homes_passed
    from ancestors group by 1
)
select
    u.month_end,
    u.node_id,
    u.node_type,
    n.node_name,
    n.parent_node_id,
    coalesce(r.region_name, 'UNKNOWN')                      as region_name,
    n.capacity                                              as port_capacity,
    pa.homes_passed,
    u.active_services,
    round(100 * div0(u.active_services, pa.homes_passed), 2) as take_up_pct,
    round(100 * div0(u.active_services, iff(u.node_type = 'SPLITTER', n.capacity, pa.homes_passed)), 2) as utilization_pct,
    u.active_services - lag(u.active_services) over (partition by u.node_id order by u.month_end) as net_adds_mom,
    iff(u.node_type = 'SPLITTER' and u.active_services >= n.capacity, true, false) as is_splitter_exhausted,
    round(st_y(n.location), 6)                              as latitude,
    round(st_x(n.location), 6)                              as longitude,
    current_timestamp()                                     as loaded_at
from used u
join {{ source('network', 'ftth_topology') }} n on n.node_id = u.node_id
left join {{ ref('slv_region') }} r on r.region_id = n.region_id
left join passed pa on pa.node_id = u.node_id
