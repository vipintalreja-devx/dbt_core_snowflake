{{ config(
    materialized='table',
    tags=['gold', 'aggregates', 'core', 'privacy', 'snapshot', 'e14']
) }}
-- Current customer consent coverage per consent type x channel x age band (Data Privacy Act reporting).
-- CHANNEL_LIST is exploded with LATERAL FLATTEN so each permitted channel is counted.
-- Ported from x_e14_consent_snapshot (PIPE_X.SP_X_E14_CONSENT_SNAPSHOT).
-- Grain   : consent_type x channel x age_band   (current state)
-- Sources : slv_customer_consent, dim_customer, dim_account, int_subscriber_profile
with active_customers as (
    select customer_id, count_if(status in ('ACTIVE', 'SUSPENDED')) as active_lines
    from {{ ref('int_subscriber_profile') }}
    group by 1
),
flat as (
    select cc.customer_id, cc.consent_type, cc.is_granted, cc.captured_at_utc,
           coalesce(upper(f.value::string), 'NONE')         as channel
    from {{ ref('slv_customer_consent') }} cc,
         lateral flatten(input => cc.channel_list, outer => true) f
),
consent as (
    select
        cc.customer_id, cc.consent_type, cc.is_granted, cc.captured_at_utc, cc.channel,
        dc.age_band, dc.is_senior, dc.is_test_customer,
        coalesce(ac.active_lines, 0)                        as active_lines
    from flat cc
    left join {{ ref('dim_customer') }} dc on dc.customer_id = cc.customer_id
    left join active_customers ac         on ac.customer_id = cc.customer_id
)
select
    current_date                                            as snapshot_date,
    consent_type, channel, coalesce(age_band, 'UNKNOWN')    as age_band,
    count(distinct customer_id)                             as customers,
    count_if(is_granted)                                    as granted,
    round(div0(count_if(is_granted), count(*)) * 100, 2)    as granted_pct,
    count_if(is_granted and active_lines > 0)               as granted_with_active_lines,
    count_if(is_senior)                                     as senior_customers,
    count_if(is_test_customer)                              as test_customers,
    max(captured_at_utc)                                    as latest_capture_utc,
    datediff('day', min(captured_at_utc), current_timestamp()) as oldest_capture_age_days,
    current_timestamp()                                     as loaded_at
from consent
group by 1, 2, 3, 4
