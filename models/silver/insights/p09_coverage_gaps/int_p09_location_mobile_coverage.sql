{{ config(
    materialized='table',
    schema='silver',
    tags=['insights', 'p09'],
    transient=true
) }}
-- P09 coverage gaps | step 2: mobile coverage of every service location.
-- Grain   : service_instance_id
-- Sources : int_p09_service_location, network.cell_site (bronze COVERAGE_POLYGON GEOMETRY),
--           slv_cell_site (cell location, radio type)
-- Method  : polygons are expanded to H3 res-8 cells (H3_COVERAGE) so the point-in-polygon test
--           (ST_CONTAINS) only runs on candidate pairs; the nearest cell is searched among sites in
--           the H3 res-6 disk (k=3, ~20 km) around each location.
with loc as (
    select service_instance_id, service_location, h3_r8, h3_r6
    from {{ ref('int_p09_service_location') }}
),
poly as (
    select c.cell_id, s.radio_type, to_geography(st_aswkt(c.coverage_polygon)) as coverage
    from {{ source('network', 'cell_site') }} c
    join {{ ref('slv_cell_site') }} s on s.cell_id = c.cell_id
    where c.coverage_polygon is not null
),
poly_h3 as (
    select p.cell_id, p.radio_type, p.coverage, f.value::number as h3_r8
    from poly p, lateral flatten(input => h3_coverage(p.coverage, 8)) f
),
covered as (
    select l.service_instance_id,
           count(distinct ph.cell_id)                       as covering_cells,
           count(distinct iff(ph.radio_type = '5G', ph.cell_id, null)) as covering_5g_cells,
           count(distinct iff(ph.radio_type = '4G', ph.cell_id, null)) as covering_4g_cells
    from loc l
    join poly_h3 ph on ph.h3_r8 = l.h3_r8
    where st_contains(ph.coverage, l.service_location)
    group by 1
),
site_h3 as (
    select cell_id, radio_type, cell_location, h3_point_to_cell(cell_location, 6) as h3_r6
    from {{ ref('slv_cell_site') }}
    where cell_location is not null
),
candidates as (
    select l.service_instance_id, s.cell_id, s.radio_type,
           st_distance(l.service_location, s.cell_location) as distance_m
    from loc l, lateral flatten(input => h3_grid_disk(l.h3_r6, 3)) d
    join site_h3 s on s.h3_r6 = d.value::number
),
nearest as (
    select service_instance_id,
           cell_id                                          as nearest_cell_id,
           radio_type                                       as nearest_cell_radio_type,
           round(distance_m)                                as nearest_cell_distance_m,
           min(iff(radio_type = '5G', distance_m, null)) over (partition by service_instance_id) as nearest_5g_distance_m_raw
    from candidates
    qualify row_number() over (partition by service_instance_id order by distance_m) = 1
)
select
    l.service_instance_id,
    coalesce(c.covering_cells, 0)                           as covering_cells,
    coalesce(c.covering_5g_cells, 0)                        as covering_5g_cells,
    coalesce(c.covering_4g_cells, 0)                        as covering_4g_cells,
    coalesce(c.covering_cells, 0) > 0                       as is_mobile_covered,
    n.nearest_cell_id,
    n.nearest_cell_radio_type,
    n.nearest_cell_distance_m,
    round(n.nearest_5g_distance_m_raw)                      as nearest_5g_distance_m,
    n.nearest_cell_id is null                               as is_beyond_search_radius
from loc l
left join covered c on c.service_instance_id = l.service_instance_id
left join nearest n on n.service_instance_id = l.service_instance_id
