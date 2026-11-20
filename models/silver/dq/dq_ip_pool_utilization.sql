{{ config(
    materialized='table',
    schema='silver',
    cluster_by=['region_id'],
    tags=['silver', 'dq', 'network', 'capacity', 't04'],
    transient=true
) }}
-- IP address pool capacity per pool: utilisation, CIDR-derived theoretical size, gateway validity,
-- elements actually addressed from the pool and exhaustion risk.
-- Ported from telco_v2_core t04_ip_pool_utilization (no-date-column table; clustered by REGION_ID).
-- Grain: IP pool.
-- Sources (4): ext.ip_pool, slv_region, dq_ip_address_validation, slv_service_instance (FTTH demand).
with pools as (
    select
        p.pool_id, p.region_id, p.cidr, p.pool_type, p.pool_size, p.used_count, p.gateway_ip, p.notes,
        try_parse_ip(p.cidr, 'INET')                            as cidr_obj,
        try_to_number(split_part(p.cidr, '/', 2))               as prefix_len
    from {{ source('ext', 'ip_pool') }} p
),
addressed as (
    select matched_pool_id as pool_id, count(*) as elements_addressed
    from {{ ref('dq_ip_address_validation') }}
    where matched_pool_id is not null
    group by 1
),
ftth_demand as (
    select c.region_id, count(*) as active_ftth_services
    from {{ ref('slv_service_instance') }} si
    join {{ ref('slv_subscription') }} s on s.subscription_id = si.subscription_id
    join {{ ref('slv_cell_site') }} c    on c.cell_id = s.home_cell_id
    where si.is_active and si.service_type = 'FTTH'
    group by 1
)
select
    p.pool_id,
    p.region_id,
    r.region_name,
    p.cidr,
    p.prefix_len,
    p.pool_type,
    p.pool_size,
    p.used_count,
    round(div0(p.used_count, p.pool_size) * 100, 2)            as utilization_pct,
    iff(p.cidr_obj:family::int = 4 and p.prefix_len between 0 and 32,
        power(2, 32 - p.prefix_len), null)                      as cidr_addresses,
    p.pool_size > iff(p.cidr_obj:family::int = 4, power(2, 32 - p.prefix_len), p.pool_size) as is_size_exceeds_cidr,
    p.cidr_obj is not null                                      as is_cidr_valid,
    p.cidr_obj:ipv4_range_start::int                            as range_start_int,
    p.cidr_obj:ipv4_range_end::int                              as range_end_int,
    try_parse_ip(p.gateway_ip, 'INET') is not null              as is_gateway_valid,
    try_parse_ip(p.gateway_ip, 'INET'):ipv4::int between p.cidr_obj:ipv4_range_start::int
                                                     and p.cidr_obj:ipv4_range_end::int as is_gateway_in_range,
    zeroifnull(a.elements_addressed)                            as elements_addressed,
    iff(p.pool_type = 'FTTH', zeroifnull(fd.active_ftth_services), null) as region_ftth_demand,
    case
        when div0(p.used_count, p.pool_size) >= 0.95 then 'EXHAUSTED'
        when div0(p.used_count, p.pool_size) >= 0.80 then 'AT_RISK'
        when div0(p.used_count, p.pool_size) >= 0.50 then 'HEALTHY'
        else 'UNDERUSED'
    end                                                         as capacity_status,
    width_bucket(div0(p.used_count, p.pool_size), 0, 1, 10)    as utilization_decile,
    ratio_to_report(p.pool_size) over (partition by p.region_id) as region_capacity_share,
    p.notes
from pools p
left join {{ ref('slv_region') }} r on r.region_id = p.region_id
left join addressed a               on a.pool_id = p.pool_id
left join ftth_demand fd            on fd.region_id = p.region_id
