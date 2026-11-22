{{ config(
    materialized='table',
    schema='silver',
    tags=['insights', 'p10'],
    transient=true,
    cluster_by=['device_id']
) }}
-- P10 device lifecycle | step 1: price observations per device over time.
-- Grain   : device_id x price_date x price_source
-- Sources : slv_device (catalogue list price at launch), s3_feeds.ext_device_feed (daily vendor
--           price updates read in place from S3)
select device_id, launch_date as price_date, 'CATALOG_LAUNCH' as price_source,
       list_price_php::number(12, 2) as price_php, null::varchar as file_name
from {{ ref('slv_device') }}
where launch_date is not null and list_price_php > 0
union all
select device_id, feed_date, 'VENDOR_FEED', price_php::number(12, 2), file_name
from {{ source('s3_feeds', 'ext_device_feed') }}
where device_id is not null and price_php > 0
qualify row_number() over (partition by device_id, feed_date order by file_name desc) = 1
