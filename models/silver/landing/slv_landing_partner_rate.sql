{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'landing', 'partner', 'p3'],
    transient=true
) }}
-- Partner wholesale rate files as loaded by COPY INTO LANDING.PARTNER_RATE_RAW (CSV, ON_ERROR=CONTINUE).
-- Ported from telco_v2_core p3_partner_rates. The file load itself stays outside dbt; this model
-- validates each loaded row against the brand master and the in-force rate card.
-- Grain: one row per loaded file row (deduplicated on brand/service/unit/effective_from/file).
-- Sources (4): landing.partner_rate_raw (bronze), s3_feeds.ext_partner_rate (S3 external table), slv_partner_brand, slv_partner_rate_card.
with raw as (
    select
        r.partner_brand_id,
        upper(trim(r.service_type))                             as service_type,
        trim(r.unit)                                            as unit,
        r.rate_php::number(18, 6)                               as rate_php,
        r.effective_from,
        r.effective_to,
        r.file_name,
        coalesce(regexp_substr(r.file_name, '[0-9]{8}'),
                 replace(regexp_substr(r.file_name, '[0-9]{4}-[0-9]{2}-[0-9]{2}'), '-', '')) as file_date_token,
        {{ tz_to_utc('r.loaded_at') }}                          as loaded_at_utc
    from (
        -- legacy COPY INTO landing table + daily partner files read in place through the S3 external table
        select partner_brand_id, service_type, unit, rate_php, effective_from, effective_to, file_name, loaded_at
          from {{ source('landing', 'partner_rate_raw') }}
        union all
        select partner_brand_id, service_type, unit, rate_php, effective_from, effective_to, file_name,
               dateadd('hour', 1, feed_date::timestamp_ltz) as loaded_at
          from {{ source('s3_feeds', 'ext_partner_rate') }}
    ) r
    qualify row_number() over (partition by r.partner_brand_id, upper(trim(r.service_type)), trim(r.unit),
                                            r.effective_from, r.file_name
                               order by r.loaded_at desc) = 1
)
select
    raw.*,
    try_to_date(raw.file_date_token, 'YYYYMMDD')                as file_date,
    b.brand_name,
    rc.rate_php                                                 as card_rate_php,
    round(raw.rate_php - rc.rate_php, 6)                        as rate_diff_vs_card_php,
    b.partner_brand_id is not null                              as is_known_brand,
    raw.partner_brand_id is not null
        and raw.rate_php > 0
        and (raw.effective_to is null or raw.effective_to >= raw.effective_from)
        and b.partner_brand_id is not null                      as is_valid,
    array_compact(array_construct(
        iff(raw.partner_brand_id is null, 'MISSING_BRAND', null),
        iff(b.partner_brand_id is null and raw.partner_brand_id is not null, 'UNKNOWN_BRAND', null),
        iff(coalesce(raw.rate_php, 0) <= 0, 'NON_POSITIVE_RATE', null),
        iff(raw.effective_to < raw.effective_from, 'INVERTED_VALIDITY', null),
        iff(rc.rate_php is not null and abs(raw.rate_php - rc.rate_php) > 0.000001, 'DIFFERS_FROM_CARD', null)
    ))                                                          as issues
from raw
left join {{ ref('slv_partner_brand') }} b
       on b.partner_brand_id = raw.partner_brand_id
left join {{ ref('slv_partner_rate_card') }} rc
       on rc.partner_brand_id = raw.partner_brand_id
      and rc.service_type = raw.service_type
      and rc.unit = raw.unit
      and rc.effective_from = raw.effective_from
