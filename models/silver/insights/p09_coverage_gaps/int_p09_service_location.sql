{{ config(
    materialized='table',
    schema='silver',
    tags=['insights', 'p09'],
    transient=true,
    cluster_by=['h3_r7']
) }}
-- P09 coverage gaps | step 1: every fixed-service location with its H3 cells, its FTTH chain
-- resolved to the serving OLT (LINE -> PORT -> SPLITTER -> CABINET -> OLT) and the nearest OLT.
-- Grain   : service_instance_id
-- Region  : region_id / region_name = dominant home region of the lines in the same city x barangay
--           (service geography in the source does not carry a region).
-- Sources : slv_service_instance (location GEOGRAPHY, barangay), network.ftth_topology (bronze;
--           only OLTs carry a location), int_subscriber_profile (home region, reportability)
with si as (
    select service_instance_id, subscription_id, service_type, ont_serial, ftth_node_id,
           is_active, barangay, city, zip_code, service_location, latitude, longitude
    from {{ ref('slv_service_instance') }}
    where service_location is not null
),
topo as (
    select node_id, parent_node_id, node_type, node_name, region_id, capacity, location
    from {{ source('network', 'ftth_topology') }}
),
olt as (
    select node_id as olt_id, node_name as olt_name, location as olt_location, region_id as olt_region_id
    from topo where node_type = 'OLT'
),
chain as (
    select si.service_instance_id,
           p.node_id  as port_id,
           s.node_id  as splitter_id,
           c.node_id  as cabinet_id,
           o.node_id  as serving_olt_id
    from si
    join topo l on l.node_id = si.ftth_node_id and l.node_type = 'LINE'
    left join topo p on p.node_id = l.parent_node_id
    left join topo s on s.node_id = p.parent_node_id
    left join topo c on c.node_id = s.parent_node_id
    left join topo o on o.node_id = c.parent_node_id and o.node_type = 'OLT'
),
nearest_olt as (
    select si.service_instance_id, o.olt_id as nearest_olt_id,
           st_distance(si.service_location, o.olt_location) as nearest_olt_distance_m
    from si cross join olt o
    qualify row_number() over (partition by si.service_instance_id order by st_distance(si.service_location, o.olt_location)) = 1
)
select
    si.service_instance_id,
    si.subscription_id,
    si.service_type,
    si.is_active,
    si.ont_serial,
    si.barangay,
    si.city,
    si.zip_code,
    si.service_location,
    si.latitude,
    si.longitude,
    h3_point_to_cell(si.service_location, 8)                as h3_r8,
    h3_point_to_cell(si.service_location, 7)                as h3_r7,
    h3_point_to_cell(si.service_location, 6)                as h3_r6,
    ch.port_id, ch.splitter_id, ch.cabinet_id, ch.serving_olt_id,
    round(st_distance(si.service_location, so.olt_location)) as serving_olt_distance_m,
    no.nearest_olt_id,
    round(no.nearest_olt_distance_m)                        as nearest_olt_distance_m,
    p.home_region_id                                        as subscriber_region_id,
    mode(p.home_region_id) over (partition by si.city, si.barangay)   as region_id,
    mode(p.home_region_name) over (partition by si.city, si.barangay) as region_name,
    coalesce(p.is_reportable, false)                        as is_reportable
from si
left join chain ch on ch.service_instance_id = si.service_instance_id
left join olt so on so.olt_id = ch.serving_olt_id
left join nearest_olt no on no.service_instance_id = si.service_instance_id
left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = si.subscription_id
