{{ config(
    materialized='incremental',
    schema='gold',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date', 'region_id'],
    tags=['insights', 'p01', 'experience'],
    post_hook=["{{ ins_retention('event_date') }}"]
) }}
-- P01 Customer experience score per line and day.
-- Grain  : subscription_id x event_date (lines with network activity that day)
-- Sources: int_p01_subscriber_cell_day (usage per serving cell), int_p01_cell_day_quality (cell score),
--          slv_outage_impact + fct_outage (minutes this line was in an outage), fct_ticket (tickets opened),
--          int_subscriber_profile (home region, plan)
-- Score  : usage-weighted cell quality, minus 2 per outage hour (max 20) and 10 per network ticket (max 20).
--          Band GOOD >= 80, FAIR >= 60, POOR < 60.
with usage as (
    select u.subscription_id, u.event_date, u.cell_id, u.usage_weight, u.calls, u.voice_min, u.data_sessions, u.data_mb,
           q.cell_quality_score
    from {{ ref('int_p01_subscriber_cell_day') }} u
    left join {{ ref('int_p01_cell_day_quality') }} q
           on q.cell_id = u.cell_id and q.event_date = u.event_date
    where u.event_date >= {{ ins_daily_lower_bound('event_date', ref('int_p01_subscriber_cell_day'), 'event_date') }}
),
per_line as (
    select
        subscription_id,
        event_date,
        count(distinct cell_id)                                         as cells_used,
        sum(calls)                                                      as calls,
        round(sum(voice_min), 2)                                        as voice_min,
        sum(data_sessions)                                              as data_sessions,
        round(sum(data_mb), 2)                                          as data_mb,
        sum(usage_weight * coalesce(cell_quality_score, 100)) / nullif(sum(usage_weight), 0) as network_score,
        sum(iff(cell_quality_score < 60, usage_weight, 0)) / nullif(sum(usage_weight), 0)   as degraded_usage_share,
        max_by(cell_id, usage_weight)                                   as main_cell_id,
        min(cell_quality_score)                                         as worst_cell_score
    from usage
    group by 1, 2
),
outage_hit as (
    select i.subscription_id, o.event_date, sum(i.impact_minutes) as outage_min
    from {{ ref('slv_outage_impact') }} i
    join {{ ref('fct_outage') }} o on o.incident_id = i.incident_id
    where o.event_date >= {{ ins_daily_lower_bound('event_date', ref('int_p01_subscriber_cell_day'), 'event_date') }}
    group by 1, 2
),
tickets as (
    select subscription_id, opened_date as event_date,
           count(*)                                                     as tickets,
           count_if(category ilike any ('%NETWORK%', '%COVERAGE%', '%SIGNAL%', '%DATA%', '%SPEED%')) as network_tickets
    from {{ ref('fct_ticket') }}
    where opened_date >= {{ ins_daily_lower_bound('event_date', ref('int_p01_subscriber_cell_day'), 'event_date') }}
    group by 1, 2
),
scored as (
    select
        l.*,
        coalesce(o.outage_min, 0)                                       as outage_min,
        coalesce(t.tickets, 0)                                          as tickets,
        coalesce(t.network_tickets, 0)                                  as network_tickets,
        greatest(0, least(100,
            l.network_score
            - least(20, coalesce(o.outage_min, 0) / 30)
            - least(20, coalesce(t.network_tickets, 0) * 10)
        ))::number(6, 2)                                                as experience_score
    from per_line l
    left join outage_hit o on o.subscription_id = l.subscription_id and o.event_date = l.event_date
    left join tickets t on t.subscription_id = l.subscription_id and t.event_date = l.event_date
)
select
    s.subscription_id,
    s.event_date,
    p.home_region_id                                                    as region_id,
    p.home_region_name                                                  as region_name,
    p.plan_id,
    p.contract_type,
    s.cells_used, s.main_cell_id, s.calls, s.voice_min, s.data_sessions, s.data_mb,
    round(s.network_score, 2)                                           as network_score,
    round(s.degraded_usage_share, 4)                                    as degraded_usage_share,
    s.worst_cell_score,
    s.outage_min, s.tickets, s.network_tickets,
    s.experience_score,
    case when s.experience_score >= 80 then 'GOOD' when s.experience_score >= 60 then 'FAIR' else 'POOR' end as experience_band
from scored s
left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = s.subscription_id
