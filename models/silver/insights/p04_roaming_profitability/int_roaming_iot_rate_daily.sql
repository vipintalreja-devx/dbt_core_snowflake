{{ config(
    materialized='incremental',
    schema='silver',
    tags=['insights', 'p04', 'roaming'],
    incremental_strategy='delete+insert',
    unique_key='rate_date',
    cluster_by=['rate_date'],
    on_schema_change='append_new_columns'
) }}
-- P04 Roaming profitability: reference roaming tariff calendar (basis for the IOT cost).
-- Purpose : the per-unit reference roaming tariff in force on each day, per MVNO brand and
--           service. The negotiated inter-operator (IOT) cost = this tariff x (1 - the visited
--           operator's discount in seed roaming_iot_discounts) -- applied in fct_roaming_margin_daily.
--           Rate versions come from the partner rate card (silver) and the daily partner rate
--           files read in place from S3 (slv_landing_partner_rate <- s3_feeds.ext_partner_rate).
--           For each day the latest version effective on that day wins (ASOF JOIN); a file
--           version beats a rate-card version with the same effective date. Retail (non-MVNO)
--           lines are priced at the median of the brands' rates that day (partner_brand_id = 0).
-- Grain   : rate_date x partner_brand_id (0 = retail median) x service_group (ROAM_DATA / ROAM_VOICE / IDD_SMS)
-- Sources : slv_partner_rate_card, slv_landing_partner_rate, dim_date
with versions as (
    -- rate card: one median rate per brand x service group x effective window
    select partner_brand_id,
           regexp_substr(service_type, '^(ROAM_DATA|ROAM_VOICE|IDD_SMS)')  as service_group,
           effective_from, effective_to,
           median(rate_php)                                                 as rate_php,
           count(*)                                                         as rate_lines,
           'RATE_CARD'                                                      as rate_source,
           1                                                                as source_priority
    from {{ ref('slv_partner_rate_card') }}
    where regexp_like(service_type, '^(ROAM_DATA|ROAM_VOICE|IDD_SMS).*')
    group by 1, 2, 3, 4
    union all
    -- daily partner rate files (S3 external table via the landing model)
    select partner_brand_id,
           regexp_substr(service_type, '^(ROAM_DATA|ROAM_VOICE|IDD_SMS)'),
           effective_from, coalesce(effective_to, '9999-12-31'::date),
           median(rate_php), count(*), 'PARTNER_FILE', 2
    from {{ ref('slv_landing_partner_rate') }}
    where is_valid and regexp_like(service_type, '^(ROAM_DATA|ROAM_VOICE|IDD_SMS).*')
    group by 1, 2, 3, 4
),
ranked as (
    select *
    from versions
    qualify row_number() over (partition by partner_brand_id, service_group, effective_from
                               order by source_priority desc, effective_to desc) = 1
),
spine as (
    select d.date_day as rate_date, b.partner_brand_id, s.service_group
    from {{ ref('dim_date') }} d
    cross join (select distinct partner_brand_id from ranked) b
    cross join (select column1 as service_group from values ('ROAM_DATA'), ('ROAM_VOICE'), ('IDD_SMS')) s
    -- 31 extra days so the last known rate can be carried into the first output day
    where d.date_day >= dateadd('day', -31, {{ f1_lower_bound('rate_date') }})
      and d.date_day <= current_date()
),
brand_rate as (
    select sp.rate_date, sp.partner_brand_id, sp.service_group,
           iff(v.effective_to >= sp.rate_date, v.rate_php, null)            as rate_php,
           iff(v.effective_to >= sp.rate_date, v.rate_source, null)         as rate_source,
           v.effective_from                                                 as rate_effective_from
    from spine sp
    asof join ranked v
        match_condition (sp.rate_date >= v.effective_from)
        on sp.partner_brand_id = v.partner_brand_id
       and sp.service_group = v.service_group
),
-- carry the last known rate forward over gaps (expired version, no successor yet)
filled as (
    select rate_date, partner_brand_id, service_group,
           coalesce(rate_php, lag(rate_php) ignore nulls over (partition by partner_brand_id, service_group order by rate_date)) as rate_php,
           coalesce(rate_source, 'CARRIED_FORWARD')                         as rate_source,
           rate_effective_from
    from brand_rate
)
select rate_date, partner_brand_id, service_group, rate_php, rate_source, rate_effective_from
from filled
where rate_php is not null and rate_date >= {{ f1_lower_bound('rate_date') }}
union all
select rate_date, 0 as partner_brand_id, service_group,
       median(rate_php)                                                     as rate_php,
       'RETAIL_MEDIAN'                                                      as rate_source,
       max(rate_effective_from)                                             as rate_effective_from
from filled
where rate_php is not null and rate_date >= {{ f1_lower_bound('rate_date') }}
group by rate_date, service_group
