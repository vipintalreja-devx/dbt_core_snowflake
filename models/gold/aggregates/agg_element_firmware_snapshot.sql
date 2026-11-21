{{ config(
    materialized='table',
    tags=['gold', 'aggregates', 'network', 'inventory', 'snapshot', 'e22']
) }}
-- Materialization: table (current-state/small reference): current-state snapshot of a small reference/master table - rebuilt in seconds.
-- Network elements per type x vendor x firmware version (current state): footprint, fault rate in
-- the window and whether the version is the vendor's latest - the firmware-upgrade backlog.
-- Ported from x_e22_element_firmware_snapshot (PIPE_X.SP_X_E22_ELEMENT_FIRMWARE_SNAPSHOT).
-- Grain   : element_type x vendor x firmware_version   (current state)
-- Sources : slv_network_element, dim_cell, slv_alarm_event, network.ne_port_inventory (bronze)
with alarms as (
    select element_id,
           count(*)                                         as alarms_window,
           count_if(severity in ('CRITICAL', 'MAJOR'))      as major_alarms_window
    from {{ ref('slv_alarm_event') }}
    group by 1
),
ports as (
    select element_id, count(*) as ports, count_if(upper(oper_status) = 'UP') as ports_up
    from {{ source('network', 'ne_port_inventory') }}
    group by 1
),
ne as (
    select
        e.element_id, e.element_type, coalesce(e.vendor, 'UNKNOWN') as vendor,
        coalesce(e.firmware_version, 'UNKNOWN')             as firmware_version,
        e.installed_at_utc, e.cell_id,
        coalesce(c.region_name, 'UNKNOWN')                  as region_name,
        zeroifnull(a.alarms_window)                         as alarms_window,
        zeroifnull(a.major_alarms_window)                   as major_alarms_window,
        zeroifnull(p.ports)                                 as ports,
        zeroifnull(p.ports_up)                              as ports_up
    from {{ ref('slv_network_element') }} e
    left join {{ ref('dim_cell') }} c on c.cell_id = e.cell_id
    left join alarms a               on a.element_id = e.element_id
    left join ports p                on p.element_id = e.element_id
),
agg as (
    select
        element_type, vendor, firmware_version,
        count(*)                                            as elements,
        count(distinct cell_id)                             as cells,
        array_agg(distinct region_name) within group (order by region_name) as regions,
        min(installed_at_utc)                               as first_installed_utc,
        max(installed_at_utc)                               as last_installed_utc,
        sum(alarms_window)                                  as alarms_window,
        sum(major_alarms_window)                            as major_alarms_window,
        sum(ports)                                          as ports,
        sum(ports_up)                                       as ports_up
    from ne
    group by 1, 2, 3
)
select
    current_date                                            as snapshot_date,
    g.*,
    round(div0(g.alarms_window, g.elements), 3)             as alarms_per_element,
    round(div0(g.ports_up, g.ports) * 100, 2)               as port_up_pct,
    round(ratio_to_report(g.elements) over (partition by g.element_type, g.vendor), 6) as share_of_vendor_fleet,
    dense_rank() over (partition by g.element_type, g.vendor order by g.firmware_version desc) as version_recency_rank,
    dense_rank() over (partition by g.element_type, g.vendor order by g.firmware_version desc) = 1 as is_latest_version,
    current_timestamp()                                     as loaded_at
from agg g
