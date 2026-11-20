{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='recharge_month',
    on_schema_change='append_new_columns',
    tags=['gold', 'analytics', 'prepaid', 'recharge', 'm15']
) }}
-- Prepaid top-up behaviour per month, plan and pack: volume, value distribution, channel mix
-- (as an OBJECT), repeat-recharge cadence and plan base penetration.
-- Ported from x_m15_recharge_plan_monthly (PIPE_X.SP_X_M15_RECHARGE_PLAN_MONTHLY).
-- Grain: recharge_month x plan_id x pack_code.
-- Sources: fct_recharge, int_subscriber_profile, slv_plan, dim_plan.
with rc as (
    select
        date_trunc('month', r.event_date)::date             as recharge_month,
        coalesce(p.plan_id, 'UNKNOWN')                      as plan_id,
        r.pack_code,
        r.recharge_channel,
        r.subscription_id,
        r.recharge_amount_php,
        r.days_since_prev_recharge
    from {{ ref('fct_recharge') }} r
    left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = r.subscription_id
    where {{ f2_month_window("date_trunc('month', r.event_date)", 'recharge_month') }}
),
channel_mix as (
    select recharge_month, plan_id, pack_code,
           object_agg(recharge_channel, recharges::variant)  as channel_mix
    from (
        select recharge_month, plan_id, pack_code, recharge_channel, count(*) as recharges
        from rc group by 1, 2, 3, 4
    )
    group by 1, 2, 3
),
agg as (
    select
        recharge_month, plan_id, pack_code,
        count(*)                                            as recharges,
        count(distinct subscription_id)                     as recharging_lines,
        sum(recharge_amount_php)                            as amount_php,
        avg(recharge_amount_php)                            as avg_amount_php,
        median(recharge_amount_php)                         as median_amount_php,
        median(days_since_prev_recharge)                    as median_days_between_recharges,
        mode(recharge_channel)                              as top_channel
    from rc
    group by 1, 2, 3
)
select
    a.recharge_month,
    a.plan_id,
    pl.plan_name,
    pl.plan_category,
    a.pack_code,
    a.recharges,
    a.recharging_lines,
    round(a.amount_php, 2)                                  as amount_php,
    round(a.avg_amount_php, 2)                              as avg_amount_php,
    a.median_amount_php,
    a.median_days_between_recharges,
    round(div0(a.recharges, a.recharging_lines), 2)         as recharges_per_line,
    a.top_channel,
    cm.channel_mix,
    dp.active_lines                                         as plan_active_lines,
    round(div0(a.recharging_lines, dp.active_lines), 4)     as plan_recharge_penetration,
    round(ratio_to_report(a.amount_php) over (partition by a.recharge_month, a.plan_id), 4) as pack_share_in_plan,
    current_timestamp()                                     as loaded_at
from agg a
left join channel_mix cm
       on cm.recharge_month = a.recharge_month and cm.plan_id = a.plan_id and cm.pack_code = a.pack_code
left join {{ ref('slv_plan') }} pl on pl.plan_id = a.plan_id
left join {{ ref('dim_plan') }} dp on dp.plan_id = a.plan_id
