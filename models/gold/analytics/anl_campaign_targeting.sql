{{ config(
    materialized='table',
    tags=['gold', 'analytics', 'marketing', 'campaigns', 'm25']
) }}
-- Campaign targeting rules exploded per targeted region (LATERAL FLATTEN on the regions JSON array)
-- and sized against the current base: how many reportable lines actually satisfy each rule.
-- Ported from x_m25_campaign_targeting_flat (PIPE_X.SP_X_M25_CAMPAIGN_TARGETING_FLAT) - all campaigns
-- live in the window instead of one AS_OF day.
-- Grain: campaign_id x region_id (region_id NULL = nationwide).
-- Sources: slv_campaign, slv_region, slv_plan, int_subscriber_profile, fct_campaign_response.
with camp as (
    select c.*
    from {{ ref('slv_campaign') }} c
    where c.start_date <= (select max(event_date) from {{ ref('fct_campaign_response') }})
      and coalesce(c.end_date, '9999-12-31'::date) >= '{{ var("history_start_date") }}'::date
),
exploded as (
    select
        c.campaign_id, c.campaign_name, c.channel, c.start_date, c.end_date, c.budget_php,
        f.value::number                                     as region_id,
        c.targeting_rules:min_tenure_months::number         as min_tenure_months,
        c.targeting_rules:churn_score_above::float          as churn_score_above,
        coalesce(c.targeting_rules:exclude_partner_brands::boolean, false) as exclude_partner_brands,
        c.targeting_rules:plans                             as target_plans,
        coalesce(array_size(c.targeting_rules:plans), 0)    as plan_count,
        c.targeting_rules:max_sends_per_sub::number         as max_sends_per_sub,
        c.targeting_rules:trigger::string                   as trigger_event
    from camp c,
         lateral flatten(input => c.targeting_rules:regions, outer => true) f
    -- some campaigns repeat a region in the JSON array
    qualify row_number() over (partition by c.campaign_id, f.value::number order by f.index) = 1
),
-- campaign plan codes (P01..P20) are checked against the plan catalogue (data-quality finding:
-- they do not match catalogue ids such as P1777); unmatched codes are ignored for sizing.
plan_codes as (
    select e.campaign_id, e.region_id, f.value::string as plan_code
    from exploded e,
         lateral flatten(input => e.target_plans, outer => true) f
),
plan_match as (
    select pc.campaign_id, pc.region_id,
           count(pc.plan_code)                              as plan_codes,
           count(pl.plan_id)                                as plan_codes_in_catalog,
           array_agg(pl.plan_id) within group (order by pl.plan_id) as matched_plan_ids
    from plan_codes pc
    left join {{ ref('slv_plan') }} pl on pl.plan_id = pc.plan_code
    group by 1, 2
),
-- compact base profile: one row per (region, tenure, churn bucket, mvno) keeps the rule-matching
-- join small (thousands of rows instead of 600k lines per campaign)
base as (
    select home_region_id as region_id, tenure_months,
           floor(coalesce(churn_probability, 0) * 100) / 100 as churn_bucket,
           is_mvno_line, count(*) as lines
    from {{ ref('int_subscriber_profile') }}
    where is_reportable and status = 'ACTIVE'
    group by 1, 2, 3, 4
),
rule_match as (
    -- regional campaigns: equi-join on region
    select e.campaign_id, e.region_id, b.lines
    from exploded e
    join base b
      on b.region_id = e.region_id
     and b.tenure_months >= coalesce(e.min_tenure_months, 0)
     and b.churn_bucket >= coalesce(e.churn_score_above, 0) - 0.005
     and (not e.exclude_partner_brands or not b.is_mvno_line)
    where e.region_id is not null
    union all
    -- nationwide campaigns (no regions array)
    select e.campaign_id, e.region_id, b.lines
    from exploded e
    join base b
      on b.tenure_months >= coalesce(e.min_tenure_months, 0)
     and b.churn_bucket >= coalesce(e.churn_score_above, 0) - 0.005
     and (not e.exclude_partner_brands or not b.is_mvno_line)
    where e.region_id is null
),
sized as (
    select campaign_id, region_id, sum(lines) as eligible_lines
    from rule_match
    group by 1, 2
),
sent as (
    select campaign_id, count(*) as sends, count_if(is_converted) as conversions
    from {{ ref('fct_campaign_response') }}
    group by 1
)
select
    e.campaign_id,
    e.region_id,
    coalesce(r.region_name, iff(e.region_id is null, 'NATIONWIDE', 'UNKNOWN')) as region_name,
    e.campaign_name,
    e.channel,
    e.start_date,
    e.end_date,
    e.budget_php,
    e.min_tenure_months,
    e.churn_score_above,
    e.exclude_partner_brands,
    e.plan_count,
    array_to_string(e.target_plans, ',')                    as target_plans,
    e.max_sends_per_sub,
    e.trigger_event,
    pm.plan_codes,
    pm.plan_codes_in_catalog,
    pm.plan_codes > pm.plan_codes_in_catalog                as has_unknown_plan_codes,
    zeroifnull(s.eligible_lines)                            as eligible_lines,
    zeroifnull(se.sends)                                    as campaign_sends,
    zeroifnull(se.conversions)                              as campaign_conversions,
    round(div0(e.budget_php, sum(zeroifnull(s.eligible_lines)) over (partition by e.campaign_id)), 4) as budget_per_eligible_line_php,
    current_timestamp()                                     as loaded_at
from exploded e
left join {{ ref('slv_region') }} r on r.region_id = e.region_id
left join sized s                   on s.campaign_id = e.campaign_id and equal_null(s.region_id, e.region_id)
left join sent se                   on se.campaign_id = e.campaign_id
left join plan_match pm             on pm.campaign_id = e.campaign_id and equal_null(pm.region_id, e.region_id)
