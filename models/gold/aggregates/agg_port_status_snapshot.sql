{{ config(
    materialized='table',
    tags=['gold', 'aggregates', 'network', 'inventory', 'snapshot', 'e23']
) }}
-- Materialization: table (current-state/small reference): current-state snapshot of a small reference/master table - rebuilt in seconds.
-- Network element ports per port type x admin status x oper status x vendor (current state):
-- capacity, optical health and admin-up/oper-down faults.
-- Ported from x_e23_port_status_snapshot (PIPE_X.SP_X_E23_PORT_STATUS_SNAPSHOT).
-- Grain   : port_type x admin_status x oper_status x vendor   (current state)
-- Sources : network.ne_port_inventory (bronze - no silver entity), slv_network_element, dim_cell
with p as (
    select
        upper(pi.port_type)                                 as port_type,
        upper(pi.admin_status)                              as admin_status,
        upper(pi.oper_status)                               as oper_status,
        pi.port_id, pi.element_id, pi.speed_mbps, pi.mtu, pi.vlan_id, pi.is_trunk,
        iff(pi.rx_power_dbm = 'NaN'::float, null, pi.rx_power_dbm) as rx_power_dbm,
        coalesce(ne.vendor, 'UNKNOWN')                      as vendor,
        ne.element_type,
        coalesce(c.region_name, 'UNKNOWN')                  as region_name
    from {{ source('network', 'ne_port_inventory') }} pi
    left join {{ ref('slv_network_element') }} ne on ne.element_id = pi.element_id
    left join {{ ref('dim_cell') }} c             on c.cell_id = ne.cell_id
)
select
    current_date                                            as snapshot_date,
    port_type, admin_status, oper_status, vendor,
    count(*)                                                as ports,
    approx_count_distinct(element_id)                       as elements_approx,
    count_if(is_trunk)                                      as trunk_ports,
    round(sum(speed_mbps) / 1000, 1)                        as capacity_gbps,
    round(avg(rx_power_dbm), 2)                             as avg_rx_power_dbm,
    min(rx_power_dbm)                                       as min_rx_power_dbm,
    count_if(rx_power_dbm < -27)                            as low_rx_ports,
    count_if(admin_status = 'UP' and oper_status = 'DOWN')  as admin_up_oper_down,
    count_if(mtu < 1500)                                    as sub_standard_mtu_ports,
    count(distinct vlan_id)                                 as vlans,
    mode(region_name)                                       as top_region,
    round(ratio_to_report(count(*)) over (partition by vendor), 6) as share_of_vendor_ports,
    current_timestamp()                                     as loaded_at
from p
group by 1, 2, 3, 4, 5
