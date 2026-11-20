{% snapshot snap_cell_config %}
{{ config(
    unique_key='cell_id',
    strategy='timestamp',
    updated_at='config_updated_at',
    invalidate_hard_deletes=True,
    tags=['snapshot', 'scd2', 'network', 'h03']
) }}
{#-
  Cell configuration, SCD Type 2 (TIMESTAMP strategy on config_updated_at).
  Ported from telco_v2_core snapshot x_h03_dim_cell_config_scd2 (PIPE_X.SP_X_H03).
  config_updated_at = latest moment any part of the cell's configuration changed (site
  commissioned, element installed, threshold re-configured), so versions follow real config events.
  Grain: one row per cell (current configuration).
  Sources (7): slv_cell_site, network_element (bronze - CONFIG JSON is not carried to silver),
  ne_port_inventory (bronze), cell_kpi_threshold (bronze - units / enabled flag),
  slv_network_element, slv_outage_incident, slv_region.
-#}
with element_summary as (
    select
        ne.cell_id,
        count(*)                                                as elements,
        count_if(ne.element_type = 'ENODEB')                    as enodebs,
        count_if(ne.element_type = 'GNODEB')                    as gnodebs,
        count_if(ne.element_type in ('ROUTER', 'SWITCH', 'OLT')) as transport_elements,
        listagg(distinct ne.vendor, ',') within group (order by ne.vendor) as vendors,
        count(distinct ne.vendor)                               as vendor_count,
        object_agg(ne.element_id::varchar, ne.firmware_version::variant) as firmware_map,
        count(distinct ne.firmware_version)                     as firmware_versions,
        array_agg(distinct raw.config:snmp.version::string)     as snmp_versions,
        count_if(raw.config:snmp.version::string <> 'v3')       as non_v3_snmp,
        min(try_to_number(raw.config:mtu::string))              as min_mtu,
        max(try_to_number(raw.config:mtu::string))              as max_mtu,
        min(array_size(raw.config:ntp))                         as min_ntp_servers,
        count_if(ne.ip_address is not null)                     as elements_with_ip,
        count_if(ne.ip_address is not null
                 and not regexp_like(ne.ip_address,
                     '^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])[.]){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$')
                 and try_parse_ip(ne.ip_address, 'INET') is null) as elements_with_bad_ip,
        max(ne.installed_at_utc)                                as last_installed_at_utc
    from {{ ref('slv_network_element') }} ne
    join {{ source('network', 'network_element') }} raw on raw.element_id = ne.element_id
    where ne.cell_id is not null
    group by ne.cell_id
),
ports as (
    select
        ne.cell_id,
        count(*)                                                as ports,
        count_if(p.port_type = '100GE')                         as ports_100ge,
        count_if(p.port_type = 'XGE')                           as ports_xge,
        count_if(p.port_type = 'GE')                            as ports_ge,
        sum(p.speed_mbps)                                       as total_port_mbps,
        count_if(p.oper_status = 'DOWN')                        as ports_down,
        count_if(p.admin_status = 'UP' and p.oper_status <> 'UP') as ports_state_mismatch,
        count_if(p.is_trunk)                                    as trunk_ports,
        count(distinct p.vlan_id)                               as vlans,
        min(p.rx_power_dbm)                                     as min_rx_power_dbm
    from {{ source('network', 'ne_port_inventory') }} p
    join {{ ref('slv_network_element') }} ne on ne.element_id = p.element_id
    group by ne.cell_id
),
thresholds as (
    select
        t.cell_id,
        count_if(t.is_enabled)                                  as enabled_thresholds,
        count_if(not t.is_enabled)                              as disabled_thresholds,
        object_agg(t.kpi_name, object_construct('warn', t.warning_threshold, 'crit', t.critical_threshold,
                                                'unit', t.unit, 'cmp', t.comparison)) as threshold_map,
        listagg(distinct trim(t.unit), ',') within group (order by trim(t.unit)) as threshold_units,
        max(t.threshold_id)                                     as latest_threshold_id
    from {{ source('network', 'cell_kpi_threshold') }} t
    group by t.cell_id
),
region_outages as (
    select region_id, max(detected_at_utc) as last_region_outage_at_utc, count(*) as region_outages
    from {{ ref('slv_outage_incident') }}
    group by region_id
)
select
    c.cell_id,
    c.site_name,
    c.radio_type,
    c.region_id,
    coalesce(r.region_name, 'UNKNOWN')                          as region_name,
    r.island_group,
    round(c.latitude, 6)                                        as latitude,
    round(c.longitude, 6)                                       as longitude,
    c.commissioned_at_utc,
    zeroifnull(es.elements)                                     as elements,
    zeroifnull(es.enodebs)                                      as enodebs,
    zeroifnull(es.gnodebs)                                      as gnodebs,
    zeroifnull(es.transport_elements)                           as transport_elements,
    es.vendors,
    zeroifnull(es.vendor_count)                                 as vendor_count,
    es.firmware_map,
    zeroifnull(es.firmware_versions)                            as firmware_versions,
    es.snmp_versions,
    zeroifnull(es.non_v3_snmp)                                  as non_v3_snmp,
    es.min_mtu,
    es.max_mtu,
    es.min_ntp_servers,
    zeroifnull(es.elements_with_ip)                             as elements_with_ip,
    zeroifnull(es.elements_with_bad_ip)                         as elements_with_bad_ip,
    zeroifnull(pt.ports)                                        as ports,
    zeroifnull(pt.ports_100ge)                                  as ports_100ge,
    zeroifnull(pt.ports_xge)                                    as ports_xge,
    zeroifnull(pt.ports_ge)                                     as ports_ge,
    zeroifnull(pt.total_port_mbps)                              as total_port_mbps,
    zeroifnull(pt.ports_down)                                   as ports_down,
    zeroifnull(pt.ports_state_mismatch)                         as ports_state_mismatch,
    zeroifnull(pt.trunk_ports)                                  as trunk_ports,
    zeroifnull(pt.vlans)                                        as vlans,
    pt.min_rx_power_dbm,
    zeroifnull(th.enabled_thresholds)                           as enabled_thresholds,
    zeroifnull(th.disabled_thresholds)                          as disabled_thresholds,
    th.threshold_map,
    th.threshold_units,
    case
        when zeroifnull(es.gnodebs) > 0 and zeroifnull(es.enodebs) > 0 then 'NSA_4G_5G'
        when zeroifnull(es.gnodebs) > 0                                then '5G_ONLY'
        when zeroifnull(es.enodebs) > 0                                then '4G_ONLY'
        else 'NO_RAN'
    end                                                         as ran_setup,
    case
        when zeroifnull(es.elements_with_bad_ip) > 0 or zeroifnull(es.non_v3_snmp) > 0 then 'NEEDS_REVIEW'
        when zeroifnull(es.firmware_versions) > 2                                      then 'FIRMWARE_DRIFT'
        else 'COMPLIANT'
    end                                                         as config_compliance,
    ro.last_region_outage_at_utc,
    zeroifnull(ro.region_outages)                               as region_outages,
    -- latest configuration event: drives the timestamp strategy
    greatest(
        coalesce(c.commissioned_at_utc,     '1900-01-01'::timestamp_ntz),
        coalesce(es.last_installed_at_utc,  '1900-01-01'::timestamp_ntz),
        dateadd('second', zeroifnull(th.latest_threshold_id) % 60, '1900-01-01'::timestamp_ntz)
    )                                                           as config_updated_at
from {{ ref('slv_cell_site') }} c
left join {{ ref('slv_region') }} r on r.region_id = c.region_id
left join element_summary es        on es.cell_id = c.cell_id
left join ports pt                  on pt.cell_id = c.cell_id
left join thresholds th             on th.cell_id = c.cell_id
left join region_outages ro         on ro.region_id = c.region_id
{% endsnapshot %}
