{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['gold', 'analytics', 'network', 'ftth', 'm18']
) }}
-- FTTH optical health per day rolled up the passive network to the CABINET (LINE -> PORT -> SPLITTER
-- -> CABINET -> OLT), with low-signal / hot ONTs and same-day connectivity tickets of the served lines.
-- Ported from x_m18_ont_health_daily (PIPE_X.SP_X_M18_ONT_HEALTH_DAILY).
-- Grain: event_date x cabinet_node_id.
-- Sources: slv_ont_health_daily, slv_service_instance, source network.ftth_topology (x4 self-join),
--          slv_region, slv_ticket.
with topo as (
    select node_id, parent_node_id, node_type, node_name, region_id, capacity
    from {{ source('network', 'ftth_topology') }}
),
line_path as (
    select
        line.node_id                                        as line_node_id,
        cab.node_id                                         as cabinet_node_id,
        cab.node_name                                       as cabinet_name,
        olt.node_id                                         as olt_node_id,
        olt.node_name                                       as olt_name,
        coalesce(cab.region_id, olt.region_id)              as region_id
    from topo line
    left join topo port on port.node_id = line.parent_node_id
    left join topo spl  on spl.node_id  = port.parent_node_id
    left join topo cab  on cab.node_id  = spl.parent_node_id
    left join topo olt  on olt.node_id  = cab.parent_node_id
    where line.node_type = 'LINE'
),
ont as (
    select
        h.event_date, h.ont_serial, h.rx_dbm_avg, h.rx_dbm_min, h.temperature_c_max, h.is_degraded, h.polls,
        si.subscription_id, lp.cabinet_node_id, lp.cabinet_name, lp.olt_node_id, lp.olt_name, lp.region_id
    from {{ ref('slv_ont_health_daily') }} h
    join {{ ref('slv_service_instance') }} si on si.ont_serial = h.ont_serial
    join line_path lp                         on lp.line_node_id = si.ftth_node_id
    where {{ incremental_date_filter('h.event_date', 'event_date') }}
),
tickets as (
    select t.opened_date as event_date, si.ont_serial, count(*) as tickets
    from {{ ref('slv_ticket') }} t
    join {{ ref('slv_service_instance') }} si on si.subscription_id = t.subscription_id and si.ont_serial is not null
    where t.category in ('NO_INTERNET', 'SLOW_SPEED')
      and {{ incremental_date_filter('t.opened_date', 'event_date') }}
    group by 1, 2
)
select
    o.event_date,
    o.cabinet_node_id,
    any_value(o.cabinet_name)                               as cabinet_name,
    any_value(o.olt_node_id)                                as olt_node_id,
    any_value(o.olt_name)                                   as olt_name,
    any_value(coalesce(r.region_name, 'UNKNOWN'))           as region_name,
    count(*)                                                as onts_reporting,
    sum(o.polls)                                            as polls,
    round(avg(o.rx_dbm_avg), 2)                             as avg_rx_dbm,
    round(min(o.rx_dbm_min), 2)                             as worst_rx_dbm,
    round(approx_percentile(o.rx_dbm_avg, 0.1), 2)          as p10_rx_dbm,
    count_if(o.is_degraded)                                 as low_signal_onts,
    count_if(o.temperature_c_max > 70)                      as hot_onts,
    round(div0(count_if(o.is_degraded), count(*)), 4)       as degraded_share,
    zeroifnull(sum(t.tickets))                              as connectivity_tickets,
    count_if(o.is_degraded and t.tickets > 0)               as degraded_onts_with_ticket,
    current_timestamp()                                     as loaded_at
from ont o
left join {{ ref('slv_region') }} r on r.region_id = o.region_id
left join tickets t on t.event_date = o.event_date and t.ont_serial = o.ont_serial
group by o.event_date, o.cabinet_node_id
