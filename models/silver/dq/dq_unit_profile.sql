{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'dq', 'network', 't02'],
    transient=true
) }}
-- Profile of KPI-threshold UNIT values (symbols, case, padding) and whether each can be converted
-- via the unit-conversion reference. Ported from telco_v2_core t02_unit_profile.
-- Grain: distinct unit value.
-- Sources (3): cell_kpi_threshold (bronze - raw unit text), ext.unit_conversion, slv_cell_site.
with u as (
    select
        t.unit,
        count(*)                                                as thresholds,
        count(distinct t.cell_id)                               as cells,
        count(distinct c.region_id)                             as regions,
        array_agg(distinct t.kpi_name) within group (order by t.kpi_name) as kpi_names,
        count_if(t.is_enabled)                                  as enabled_thresholds
    from {{ source('network', 'cell_kpi_threshold') }} t
    left join {{ ref('slv_cell_site') }} c on c.cell_id = t.cell_id
    group by t.unit
),
conv as (
    select from_unit, trim(from_unit) as from_unit_trimmed, to_unit, factor, offset_value, description,
           count(*) over (partition by trim(lower(from_unit))) as variants_of_same_unit
    from {{ source('ref', 'unit_conversion') }}
)
select
    u.unit,
    u.thresholds,
    u.cells,
    u.regions,
    u.enabled_thresholds,
    u.kpi_names,
    length(u.unit)                                              as unit_chars,
    upper(u.unit)                                               as unit_upper,
    lower(u.unit)                                               as unit_lower,
    trim(u.unit)                                                as unit_trimmed,
    u.unit <> trim(u.unit)                                      as has_padding,
    regexp_like(u.unit, '.*[^ -~].*')                           as has_non_ascii,
    hex_encode(u.unit)                                          as unit_hex,
    case when u.unit is null                          then 'MISSING'
         when u.unit in ('%', '‰')                    then 'RATIO'
         when lower(u.unit) like '%bit/s'             then 'THROUGHPUT'
         when u.unit like '°%'                        then 'TEMPERATURE'
         when lower(trim(u.unit)) = 'dbm'             then 'POWER'
         when u.unit in ('ms', 'µs', '"ms"')          then 'TIME'
         else 'OTHER' end                                       as unit_category,
    c.to_unit,
    c.factor,
    c.offset_value,
    coalesce(c.variants_of_same_unit, 0)                        as conversion_variants,
    c.from_unit is not null                                     as has_exact_conversion,
    ct.from_unit is not null and c.from_unit is null            as converts_only_after_trim
from u
left join conv c  on c.from_unit = u.unit
left join conv ct on ct.from_unit_trimmed = trim(u.unit) and ct.from_unit <> u.unit
qualify row_number() over (partition by u.unit order by c.to_unit nulls last, ct.to_unit nulls last) = 1
