{{ config(
    materialized='table',
    schema='silver',
    cluster_by=['ip_class'],
    tags=['silver', 'dq', 'network', 't01'],
    transient=true
) }}
-- Network-element management IP quality: classifies every element IP as IPV4 / IPV6 / INVALID /
-- MISSING / EMPTY, names the format defect, and checks the IP against the region's IP pools.
-- Ported from telco_v2_core t01_ip_address_validation (migration test rule), extended with
-- Snowflake PARSE_IP and pool-membership checks so it is usable as an operational DQ feed.
-- Grain: network element.
-- Sources (5): slv_network_element, slv_cell_site, ext.ip_pool, slv_region, network_element (bronze, raw IP text).
with ne as (
    select
        s.element_id,
        s.element_type,
        s.vendor,
        s.cell_id,
        raw.ip_address                                          as ip_address,      -- untouched bronze text
        c.region_id
    from {{ ref('slv_network_element') }} s
    join {{ source('network', 'network_element') }} raw on raw.element_id = s.element_id
    left join {{ ref('slv_cell_site') }} c on c.cell_id = s.cell_id
),
classified as (
    select
        ne.*,
        length(ip_address)                                      as ip_chars,
        regexp_count(ip_address, '\\.')                         as dot_count,
        regexp_count(ip_address, ':')                           as colon_count,
        case
            when ip_address is null then 'MISSING'
            when ip_address = ''    then 'EMPTY'
            when regexp_like(ip_address, '^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])[.]){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$') then 'IPV4'
            when regexp_like(ip_address, '^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$') then 'IPV6'
            when regexp_like(ip_address, '^[0-9a-fA-F:]+$') and position('::' in ip_address) > 0
                 and position(':::' in ip_address) = 0 and regexp_count(ip_address, '::') = 1
                 and regexp_count(ip_address, ':') <= 7 then 'IPV6'
            else 'INVALID'
        end                                                     as ip_class,
        case
            when ip_address is null or ip_address = ''                          then null
            when ip_address <> trim(ip_address, ' \t\n\r')                      then 'LEADING_OR_TRAILING_SPACE'
            when not regexp_like(ip_address, '^[0-9a-fA-F.:/ -]+$')             then 'SPECIAL_OR_NON_ASCII_CHARACTERS'
            when position('/' in ip_address) > 0                                then 'CIDR_SUFFIX'
            when right(ip_address, 1) = '.' or position('..' in ip_address) > 0
                 or (position(':' in ip_address) = 0 and regexp_count(ip_address, '\\.') < 3) then 'INCOMPLETE'
            when position(':' in ip_address) = 0 and regexp_count(ip_address, '\\.') > 3      then 'TOO_MANY_OCTETS'
            when position(':' in ip_address) = 0
                 and try_parse_ip(ip_address, 'INET') is null                   then 'OCTET_OUT_OF_RANGE'
            else null
        end                                                     as format_issue,
        trim(ip_address, ' \t\n\r')                             as ip_trimmed
    from ne
),
parsed as (
    select
        c.*,
        try_parse_ip(iff(c.ip_class in ('IPV4', 'IPV6'), c.ip_address, null), 'INET') as ip_obj
    from classified c
),
pools as (
    select pool_id, region_id, cidr, pool_type, try_parse_ip(cidr, 'INET') as pool_obj
    from {{ source('network', 'ip_pool') }}
),
pool_match as (
    -- which of the element's regional pools (if any) contains its IPv4 address
    select p.element_id,
           min(pl.pool_id)                                      as matched_pool_id,
           count(pl.pool_id)                                    as matching_pools
    from parsed p
    join pools pl
      on pl.region_id = p.region_id
     and p.ip_obj:family::int = 4
     and pl.pool_obj:family::int = 4
     and p.ip_obj:ipv4::int between pl.pool_obj:ipv4_range_start::int and pl.pool_obj:ipv4_range_end::int
    group by p.element_id
)
select
    p.element_id,
    p.element_type,
    p.vendor,
    p.cell_id,
    p.region_id,
    r.region_name,
    p.ip_address,
    p.ip_trimmed,
    p.ip_chars,
    p.dot_count,
    p.colon_count,
    p.ip_class,
    p.format_issue,
    p.ip_obj:family::int                                        as ip_family,
    p.ip_obj:host::string                                       as ip_canonical,
    pm.matched_pool_id,
    zeroifnull(pm.matching_pools)                               as matching_pools,
    p.ip_class in ('IPV4', 'IPV6')                              as is_valid_ip,
    p.ip_class = 'IPV4' and pm.matched_pool_id is null          as is_outside_regional_pool,
    hash(p.ip_address)                                          as ip_hash
from parsed p
left join pool_match pm on pm.element_id = p.element_id
left join {{ ref('slv_region') }} r on r.region_id = p.region_id
