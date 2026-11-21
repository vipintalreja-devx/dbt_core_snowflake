{{ config(
    materialized='table',
    schema='silver',
    cluster_by=['device_id'],
    tags=['silver', 'landing', 'device', 'p4'],
    transient=true
) }}
-- Vendor device feed (JSON files COPY'd into LANDING.DEVICE_FEED_RAW), flattened to one row per
-- device x specification key, and compared with the device catalogue.
-- Ported from telco_v2_core p4_device_feed (was a view with LATERAL FLATTEN over SPECS).
-- Grain: device_id x spec_key (array-valued specs are kept as JSON text + element count).
-- Sources (3): landing.device_feed_raw (bronze), s3_feeds.ext_device_feed (S3 external table), slv_device.
with feed as (
    select
        f.raw:device_id::number                                 as device_id,
        f.raw:maker::string                                     as manufacturer,
        f.raw:model::string                                     as model,
        upper(f.raw:type::string)                               as device_type,
        f.raw:supports_5g::boolean                              as supports_5g,
        f.raw:price_php::number(12, 2)                          as list_price_php,
        f.raw:specs                                             as specs,
        f.file_name,
        {{ tz_to_utc('f.loaded_at') }}                          as loaded_at_utc
    from (
        -- legacy COPY INTO landing table + daily vendor files read in place through the S3 external table
        select raw, file_name, loaded_at from {{ source('landing', 'device_feed_raw') }}
        union all
        select raw, file_name, dateadd('hour', 1, feed_date::timestamp_ltz) as loaded_at
          from {{ source('s3_feeds', 'ext_device_feed') }}
    ) f
    qualify row_number() over (partition by f.raw:device_id::number order by f.loaded_at desc) = 1
),
flat as (
    select fd.*, s.key as spec_key, s.value as spec_raw
    from feed fd,
         lateral flatten(input => fd.specs, outer => true) s
)
select
    fl.device_id,
    fl.manufacturer,
    fl.model,
    fl.device_type,
    fl.supports_5g,
    fl.list_price_php,
    fl.spec_key,
    typeof(fl.spec_raw)                                         as spec_value_type,
    iff(is_array(fl.spec_raw), to_json(fl.spec_raw), fl.spec_raw::string) as spec_value,
    iff(is_array(fl.spec_raw), array_size(fl.spec_raw), null)   as spec_array_size,
    try_to_double(fl.spec_raw::string)                          as spec_value_number,
    -- feed vs catalogue reconciliation
    d.device_id is not null                                     as is_in_catalog,
    not equal_null(d.list_price_php, fl.list_price_php)         as is_price_changed,
    not equal_null(d.supports_5g, fl.supports_5g)               as is_5g_flag_changed,
    d.list_price_php                                            as catalog_price_php,
    fl.file_name,
    fl.loaded_at_utc
from flat fl
left join {{ ref('slv_device') }} d on d.device_id = fl.device_id
where fl.spec_key is null or fl.spec_key <> 'certificate'     -- opaque blob, not a specification
