{{ config(
    materialized='table',
    schema='gold',
    tags=['insights', 'p09'],
    transient=false,
    copy_grants=true
) }}
-- P09 coverage gaps | output 2: H3 resolution-7 hexagon grid for coverage maps (current state).
-- Grain   : h3_r7 (~5 km2 hexagons)
-- Sources : int_p09_service_location, int_p09_location_mobile_coverage, slv_cell_site,
--           fct_p09_ont_health_barangay_daily is not hex-addressable, so ONT health comes from the
--           lines' barangays via mart_p09_coverage_gaps
with lines as (
    select l.h3_r7, l.service_type, l.region_id, l.region_name, l.barangay, l.city,
           c.is_mobile_covered, c.covering_5g_cells, c.nearest_cell_distance_m
    from {{ ref('int_p09_service_location') }} l
    left join {{ ref('int_p09_location_mobile_coverage') }} c on c.service_instance_id = l.service_instance_id
    where l.is_active and l.is_reportable
),
sites as (
    select h3_point_to_cell(cell_location, 7) as h3_r7,
           count(*) as cell_sites, count_if(radio_type = '5G') as sites_5g
    from {{ ref('slv_cell_site') }}
    group by 1
),
brgy as (
    select region_id, city, barangay, ftth_degraded_share_30d, gap_type
    from {{ ref('mart_p09_coverage_gaps') }}
)
select
    l.h3_r7,
    h3_int_to_string(l.h3_r7)                               as h3_r7_hex,
    st_aswkt(h3_cell_to_boundary(l.h3_r7))                  as hex_boundary_wkt,
    mode(l.region_id)                                       as region_id,
    mode(l.region_name)                                     as region_name,
    count(*)                                                as active_lines,
    count_if(l.service_type = 'FTTH')                       as ftth_lines,
    count(distinct l.barangay)                              as barangays,
    round(avg(iff(l.is_mobile_covered, 1, 0)), 4)           as mobile_covered_share,
    round(avg(iff(l.covering_5g_cells > 0, 1, 0)), 4)       as covered_5g_share,
    round(avg(l.nearest_cell_distance_m))                   as avg_nearest_cell_m,
    coalesce(any_value(s.cell_sites), 0)                    as cell_sites_in_hex,
    coalesce(any_value(s.sites_5g), 0)                      as sites_5g_in_hex,
    round(avg(b.ftth_degraded_share_30d), 4)                as ftth_degraded_share_30d,
    mode(b.gap_type)                                        as dominant_gap_type,
    current_timestamp()                                     as loaded_at
from lines l
left join sites s on s.h3_r7 = l.h3_r7
left join brgy b on b.region_id = l.region_id and b.city = l.city and b.barangay = l.barangay
group by l.h3_r7
