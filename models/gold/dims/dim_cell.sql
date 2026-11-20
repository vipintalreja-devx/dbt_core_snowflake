{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dims', 'dimension'],
    transient=false,
    copy_grants=true,
    post_hook="alter table {{ this }} set comment = 'Gold dimension - dim_cell (rebuilt {{ run_started_at.strftime(\'%Y-%m-%d %H:%M\') }} UTC)'"
) }}
with ne as (
    select cell_id, count(*) as network_elements, listagg(distinct vendor, ',') as vendors
    from {{ ref('slv_network_element') }}
    group by 1
)
select
    c.cell_id, c.site_name, c.region_id, coalesce(c.region_name, 'UNKNOWN') as region_name, c.island_group,
    c.radio_type, c.latitude, c.longitude, c.commissioned_at_utc,
    coalesce(ne.network_elements, 0) as network_elements, ne.vendors,
    t.drop_rate_pct_warn, t.drop_rate_pct_crit, t.prb_pct_warn, t.prb_pct_crit,
    t.latency_ms_warn, t.latency_ms_crit, t.throughput_mbps_warn, t.throughput_mbps_crit
from {{ ref('slv_cell_site') }} c
left join ne on ne.cell_id = c.cell_id
left join {{ ref('slv_cell_kpi_threshold') }} t on t.cell_id = c.cell_id
