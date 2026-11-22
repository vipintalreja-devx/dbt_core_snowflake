{{ config(
    materialized='table',
    schema='gold',
    tags=['insights', 'p09'],
    transient=false,
    copy_grants=true
) }}
-- P09 coverage gaps | output 1: coverage and fixed-network gap per barangay (current state).
-- Grain   : city x barangay (region_id = dominant region of the barangay)
-- Sources : int_p09_service_location, int_p09_location_mobile_coverage,
--           fct_p09_ont_health_barangay_daily (last 30 days)
-- Gap types: NO_MOBILE_COVERAGE (most lines outside every cell polygon), FIBER_EXPANSION (DSL lines
--           within 2 km of an OLT), FTTH_QUALITY (degraded optical health), FAR_FROM_SITE, OK.
{% set ont_from = p_max_date_literal(ref('fct_p09_ont_health_barangay_daily'), 'event_date', 30) %}
with lines as (
    select l.*, c.is_mobile_covered, c.covering_cells, c.covering_5g_cells, c.nearest_cell_distance_m,
           c.nearest_5g_distance_m, c.nearest_cell_radio_type
    from {{ ref('int_p09_service_location') }} l
    left join {{ ref('int_p09_location_mobile_coverage') }} c on c.service_instance_id = l.service_instance_id
    where l.is_active and l.is_reportable
),
ont as (
    select region_id, city, barangay,
           sum(onts_degraded) / nullif(sum(onts_reporting), 0) as ftth_degraded_share_30d,
           avg(rx_dbm_avg)                                  as rx_dbm_avg_30d
    from {{ ref('fct_p09_ont_health_barangay_daily') }}
    where event_date >= {{ ont_from }}
    group by 1, 2, 3
),
agg as (
    select
        region_id, region_name, city, barangay,
        count(*)                                            as active_lines,
        count_if(service_type = 'FTTH')                     as ftth_lines,
        count_if(service_type = 'DSL')                      as dsl_lines,
        round(avg(iff(is_mobile_covered, 1, 0)), 4)         as mobile_covered_share,
        round(avg(iff(covering_5g_cells > 0, 1, 0)), 4)     as covered_5g_share,
        round(avg(nearest_cell_distance_m))                 as avg_nearest_cell_m,
        round(approx_percentile(nearest_cell_distance_m, 0.9)) as p90_nearest_cell_m,
        round(avg(nearest_5g_distance_m))                   as avg_nearest_5g_m,
        round(avg(iff(service_type = 'FTTH', serving_olt_distance_m, null))) as avg_ftth_drop_m,
        count_if(service_type = 'DSL' and nearest_olt_distance_m <= 2000)   as dsl_lines_within_2km_olt,
        st_centroid(st_collect(service_location))           as barangay_centroid
    from lines
    group by 1, 2, 3, 4
),
scored as (
    select a.*,
           o.ftth_degraded_share_30d,
           round(o.rx_dbm_avg_30d, 2)                       as rx_dbm_avg_30d,
           h3_point_to_cell(a.barangay_centroid, 7)         as h3_r7,
           round(100 * (
                 0.35 * (1 - a.mobile_covered_share)
               + 0.25 * least(coalesce(a.avg_nearest_cell_m, 20000) / 20000, 1)
               + 0.20 * coalesce(o.ftth_degraded_share_30d, 0)
               + 0.20 * (a.dsl_lines_within_2km_olt / nullif(a.active_lines, 0))), 1) as gap_score
    from agg a
    left join ont o on o.region_id = a.region_id and o.city = a.city and o.barangay = a.barangay
)
select
    s.region_id, s.region_name, s.city, s.barangay, s.h3_r7,
    st_aswkt(s.barangay_centroid)                           as barangay_centroid_wkt,
    s.active_lines, s.ftth_lines, s.dsl_lines,
    s.mobile_covered_share, s.covered_5g_share, s.avg_nearest_cell_m, s.p90_nearest_cell_m, s.avg_nearest_5g_m,
    s.avg_ftth_drop_m, s.dsl_lines_within_2km_olt, s.ftth_degraded_share_30d, s.rx_dbm_avg_30d,
    s.gap_score,
    case
        when s.mobile_covered_share < 0.5                    then 'NO_MOBILE_COVERAGE'
        when s.dsl_lines_within_2km_olt >= 0.3 * s.dsl_lines and s.dsl_lines > 0 then 'FIBER_EXPANSION'
        when s.ftth_degraded_share_30d >= 0.15               then 'FTTH_QUALITY'
        when s.avg_nearest_cell_m > 5000                     then 'FAR_FROM_SITE'
        else 'OK'
    end                                                     as gap_type,
    rank() over (order by s.gap_score desc)                 as national_gap_rank,
    rank() over (partition by s.region_id order by s.gap_score desc) as region_gap_rank,
    current_timestamp()                                     as loaded_at
from scored s
