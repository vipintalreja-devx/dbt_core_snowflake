{{ config(
    materialized='incremental',
    schema='silver',
    tags=['insights', 'p11'],
    incremental_strategy='delete+insert',
    unique_key='send_month',
    on_schema_change='append_new_columns',
    cluster_by=['send_month'],
    transient=true
) }}
-- P11 campaign incrementality | step 1: treated lines per campaign and send month, with the
-- stratum used to match them to a control group.
-- Grain   : campaign_id x subscription_id x send_month
-- Sources : slv_campaign_response (CDC silver), int_subscriber_profile (stratum attributes;
--           current profile is used as the stratum - an approximation for older months)
with resp as (
    select campaign_id, subscription_id,
           date_trunc('month', event_date)::date            as send_month,
           min(event_date)                                  as first_send_date,
           count(*)                                         as sends,
           boolor_agg(is_opened)                            as is_opened,
           boolor_agg(is_converted)                         as is_converted
    from {{ ref('slv_campaign_response') }}
    where {{ f2_month_window('event_date', 'send_month') }}
    group by 1, 2, 3
)
select
    r.*,
    coalesce(p.home_region_id, -1)                          as stratum_region_id,
    p.plan_category                                         as stratum_plan_category,
    coalesce(p.churn_band, 'UNSCORED')                      as stratum_churn_band,
    {{ p_tenure_band('p.tenure_months') }}                  as stratum_tenure_band,
    concat_ws('|', coalesce(p.home_region_id, -1)::varchar, p.plan_category,
              coalesce(p.churn_band, 'UNSCORED'), {{ p_tenure_band('p.tenure_months') }}) as stratum_key
from resp r
join {{ ref('int_subscriber_profile') }} p on p.subscription_id = r.subscription_id
where p.is_reportable
