{{ config(
    materialized='table',
    schema='silver',
    cluster_by=['kpi_name'],
    tags=['silver', 'dq', 'network', 't05'],
    transient=true
) }}
-- KPI thresholds converted to their alternate unit via the unit-conversion reference, plus a check
-- that the conversion round-trips and that silver's normalised thresholds agree with bronze.
-- Ported from telco_v2_core t05_threshold_unit_conversion (join on special-character UNIT keys).
-- Grain: threshold row x target unit.
-- Sources (4): cell_kpi_threshold (bronze), ext.unit_conversion, slv_cell_kpi_threshold, dim_cell.
with conv as (
    select from_unit, to_unit, factor, offset_value from {{ source('ext', 'unit_conversion') }}
)
select
    t.threshold_id,
    t.cell_id,
    dc.region_name,
    dc.radio_type,
    t.kpi_name,
    t.comparison,
    t.unit,
    c.to_unit,
    c.factor,
    c.offset_value,
    t.warning_threshold,
    t.critical_threshold,
    round(t.warning_threshold  * c.factor + c.offset_value, 6)  as warning_converted,
    round(t.critical_threshold * c.factor + c.offset_value, 6)  as critical_converted,
    -- inverse conversion must give back the original value
    abs(div0((t.warning_threshold * c.factor + c.offset_value) - c.offset_value, c.factor)
        - t.warning_threshold) < 0.000001                       as is_round_trip_ok,
    iff(t.comparison = '<', t.critical_threshold <= t.warning_threshold,
                            t.critical_threshold >= t.warning_threshold) as is_threshold_order_ok,
    case t.kpi_name
        when 'DROP_CALL_RATE'  then equal_null(s.drop_rate_pct_warn, t.warning_threshold)
        when 'PRB_UTILIZATION' then equal_null(s.prb_pct_warn, t.warning_threshold)
        when 'LATENCY'         then equal_null(s.latency_ms_warn, t.warning_threshold)
        when 'AVG_THROUGHPUT'  then equal_null(s.throughput_mbps_warn, t.warning_threshold)
    end                                                         as matches_silver_threshold,
    t.is_enabled
from {{ source('network', 'cell_kpi_threshold') }} t
join conv c                                    on c.from_unit = t.unit
left join {{ ref('slv_cell_kpi_threshold') }} s on s.cell_id = t.cell_id
left join {{ ref('dim_cell') }} dc              on dc.cell_id = t.cell_id
