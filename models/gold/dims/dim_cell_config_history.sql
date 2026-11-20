{{ config(
    materialized='table',
    schema='gold',
    cluster_by=['is_current', 'region_id'],
    tags=['gold', 'dims', 'scd2', 'network', 'h03'],
    transient=false
) }}
-- SCD Type 2 cell configuration dimension over snapshot snap_cell_config (timestamp strategy).
-- Ported from telco_v2_core x_h03_dim_cell_config_scd2. Grain: cell x configuration version.
-- Joined with the latest quality day from fct_cell_kpi_daily so config drift can be related to KPIs.
-- Sources: snap_cell_config, fct_cell_kpi_daily, dim_cell.
with latest_kpi as (
    select cell_id, event_date as last_kpi_date, is_degraded as last_day_degraded,
           drop_rate_pct_avg as last_day_drop_rate_pct, prb_pct_avg as last_day_prb_pct
    from {{ ref('fct_cell_kpi_daily') }}
    qualify row_number() over (partition by cell_id order by event_date desc) = 1
)
select
    {{ dbt_utils.generate_surrogate_key(['s.cell_id', 's.dbt_valid_from']) }} as cell_config_version_key,
    s.cell_id,
    row_number() over (partition by s.cell_id order by s.dbt_valid_from) as version_no,
    s.site_name,
    s.radio_type,
    s.region_id,
    s.region_name,
    s.island_group,
    s.elements,
    s.enodebs,
    s.gnodebs,
    s.transport_elements,
    s.vendors,
    s.vendor_count,
    s.firmware_versions,
    s.firmware_map,
    s.non_v3_snmp,
    s.min_mtu,
    s.max_mtu,
    s.elements_with_bad_ip,
    s.ports,
    s.ports_down,
    s.ports_state_mismatch,
    round(div0(s.ports_down, s.ports) * 100, 2)                 as ports_down_pct,
    s.total_port_mbps,
    s.enabled_thresholds,
    s.disabled_thresholds,
    s.threshold_map,
    s.ran_setup,
    s.config_compliance,
    c.network_elements                                          as current_network_elements,
    k.last_kpi_date,
    k.last_day_degraded,
    k.last_day_drop_rate_pct,
    k.last_day_prb_pct,
    s.config_updated_at,
    s.dbt_valid_from                                            as valid_from,
    coalesce(s.dbt_valid_to, '9999-12-31'::timestamp_ntz)       as valid_to,
    s.dbt_valid_to is null                                      as is_current
from {{ ref('snap_cell_config') }} s
left join {{ ref('dim_cell') }} c on c.cell_id = s.cell_id
left join latest_kpi k            on k.cell_id = s.cell_id and s.dbt_valid_to is null
