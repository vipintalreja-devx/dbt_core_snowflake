{{ config(
    materialized='table',
    schema='gold',
    tags=['insights', 'p08'],
    transient=false,
    copy_grants=true
) }}
-- P08 cell site ROI | step 4: capacity upgrade priority list (current state, rebuilt daily).
-- Grain   : cell_id
-- Sources : fct_p08_cell_monthly (last complete month + trend), fct_cell_kpi_daily (90-day PRB trend),
--           slv_cell_site (geography for neighbour load), dim_cell
-- Logic   : load = 90-day average of the daily PRB utilisation (peak PRB is saturated in this data,
--           so it cannot rank cells); load percentile ranks every cell against the network;
--           PRB runway = days until the daily-average PRB reaches 70% at the current 90-day slope
--           (REGR_SLOPE); neighbours within 3 km (ST_DWITHIN) decide between adding carriers and a
--           new site; the priority score blends revenue at stake, load, growth, runway and alarms.
{% set kpi_from = p_max_date_literal(ref('fct_cell_kpi_daily'), 'event_date', 90) %}
with latest_month as (
    select max(month_start) as month_start
    from {{ ref('fct_p08_cell_monthly') }}
    where kpi_days >= 20
),
m as (
    select f.*
    from {{ ref('fct_p08_cell_monthly') }} f
    join latest_month l on l.month_start = f.month_start
),
trend as (
    select cell_id,
           count(*)                                         as trend_days,
           avg(prb_pct_avg)                                 as prb_avg_90d,
           regr_slope(prb_pct_avg, datediff('day', '2000-01-01'::date, event_date)) as prb_avg_slope_per_day,
           regr_r2(prb_pct_avg, datediff('day', '2000-01-01'::date, event_date))    as prb_avg_trend_r2,
           count_if(is_congested)                           as congested_days_90d,
           count_if(is_degraded)                            as degraded_days_90d
    from {{ ref('fct_cell_kpi_daily') }}
    where event_date >= {{ kpi_from }}
    group by 1
),
geo as (
    select cell_id, cell_location, region_id from {{ ref('slv_cell_site') }}
),
neighbours as (
    select a.cell_id,
           count(*)                                         as neighbours_3km,
           avg(t.prb_avg_90d)                               as neighbour_prb_avg_90d
    from geo a
    join geo b on b.region_id = a.region_id and b.cell_id <> a.cell_id
              and st_dwithin(a.cell_location, b.cell_location, 3000)
    left join trend t on t.cell_id = b.cell_id
    group by 1
),
base as (
    select
        m.cell_id, m.site_name, m.region_id, m.region_name, m.island_group, m.radio_type,
        m.month_start                                       as assessed_month,
        m.home_lines, m.home_revenue_php, m.data_gb, m.data_gb_mom_growth, m.revenue_mom_growth,
        m.prb_pct_p95, m.congested_day_share, m.major_alarms, m.revenue_per_gb_php,
        round(t.prb_avg_90d, 2)                             as prb_avg_90d,
        t.prb_avg_slope_per_day, t.prb_avg_trend_r2, t.congested_days_90d, t.degraded_days_90d,
        case
            when t.prb_avg_90d >= 70 then 0
            when t.prb_avg_slope_per_day > 0 then round((70 - t.prb_avg_90d) / t.prb_avg_slope_per_day)
        end                                                 as prb_runway_days,
        coalesce(n.neighbours_3km, 0)                       as neighbours_3km,
        round(n.neighbour_prb_avg_90d, 2)                   as neighbour_prb_avg_90d,
        h3_latlng_to_cell(c.latitude, c.longitude, 7)       as h3_r7
    from m
    left join trend t on t.cell_id = m.cell_id
    left join neighbours n on n.cell_id = m.cell_id
    left join {{ ref('dim_cell') }} c on c.cell_id = m.cell_id
),
ranked as (
    select b.*,
           round(percent_rank() over (order by coalesce(b.prb_avg_90d, 0)), 4)          as load_pctile,
           round(percent_rank() over (order by coalesce(b.neighbour_prb_avg_90d, 0)), 4) as neighbour_load_pctile
    from base b
),
scored as (
    select b.*,
           round(b.home_revenue_php * greatest(b.load_pctile - 0.5, 0) * 2, 2) as revenue_at_risk_php,
           round(100 * (
                 0.35 * percent_rank() over (order by b.home_revenue_php * greatest(b.load_pctile - 0.5, 0))
               + 0.25 * b.load_pctile
               + 0.20 * percent_rank() over (order by coalesce(b.data_gb_mom_growth, 0))
               + 0.10 * percent_rank() over (order by -coalesce(b.prb_runway_days, 9999))
               + 0.10 * percent_rank() over (order by b.major_alarms)), 1) as priority_score
    from ranked b
)
select
    s.*,
    case
        when s.load_pctile >= 0.9 and s.neighbour_load_pctile >= 0.75         then 'NEW_SITE'
        when s.load_pctile >= 0.9 and s.radio_type <> '5G'                    then 'UPGRADE_TO_5G'
        when s.load_pctile >= 0.9 or s.prb_runway_days <= 180                 then 'ADD_CARRIER'
        when s.major_alarms > 0 and s.degraded_days_90d > 0                   then 'OPTIMISE'
        else 'MONITOR'
    end                                                     as recommended_action,
    rank() over (order by s.priority_score desc)            as national_rank,
    rank() over (partition by s.region_id order by s.priority_score desc) as region_rank,
    current_timestamp()                                     as loaded_at
from scored s
