{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='month_start',
    on_schema_change='append_new_columns',
    tags=['gold', 'analytics', 'marketing', 'segments', 'm11']
) }}
-- Marketing-segment migration matrix per month (segment at start vs. end of month) with the
-- revenue and churn behaviour of each flow.
-- Ported from x_m11_segment_transition_monthly (PIPE_X.SP_X_M11_SEGMENT_TRANSITION_MONTHLY).
-- Grain: month_start x from_segment x to_segment.
-- Sources: slv_segment_membership, fct_subscriber_monthly, int_subscriber_profile.
with months as (
    select distinct month_start
    from {{ ref('fct_subscriber_monthly') }}
    where {{ f2_month_window('month_start') }}
),
seg_bom as (
    select m.month_start, s.subscription_id, s.segment_code
    from months m
    join {{ ref('slv_segment_membership') }} s
      on m.month_start between s.valid_from and s.valid_to
    qualify row_number() over (partition by m.month_start, s.subscription_id order by s.valid_from desc) = 1
),
seg_eom as (
    select m.month_start, s.subscription_id, s.segment_code
    from months m
    join {{ ref('slv_segment_membership') }} s
      on last_day(m.month_start) between s.valid_from and s.valid_to
    qualify row_number() over (partition by m.month_start, s.subscription_id order by s.valid_from desc) = 1
),
moves as (
    select
        coalesce(b.month_start, e.month_start)              as month_start,
        coalesce(b.subscription_id, e.subscription_id)      as subscription_id,
        coalesce(b.segment_code, '(NONE)')                  as from_segment,
        coalesce(e.segment_code, '(NONE)')                  as to_segment
    from seg_bom b
    full outer join seg_eom e
      on e.month_start = b.month_start and e.subscription_id = b.subscription_id
)
select
    mv.month_start,
    mv.from_segment,
    mv.to_segment,
    mv.from_segment <> mv.to_segment                        as is_transition,
    count(*)                                                as lines,
    count_if(f.is_churned)                                  as churned_lines,
    round(sum(f.total_revenue_php), 2)                      as revenue_php,
    round(avg(f.total_revenue_php), 2)                      as avg_revenue_php,
    round(avg(f.data_mb) / 1024, 3)                         as avg_data_gb,
    round(avg(p.churn_probability), 4)                      as avg_churn_probability,
    round(ratio_to_report(count(*)) over (partition by mv.month_start, mv.from_segment), 4) as share_of_from_segment,
    current_timestamp()                                     as loaded_at
from moves mv
left join {{ ref('fct_subscriber_monthly') }} f
       on f.subscription_id = mv.subscription_id and f.month_start = mv.month_start
left join {{ ref('int_subscriber_profile') }} p
       on p.subscription_id = mv.subscription_id
group by 1, 2, 3, 4
