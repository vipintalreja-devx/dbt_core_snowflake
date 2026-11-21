{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='opened_date',
    on_schema_change='append_new_columns',
    cluster_by=['opened_date'],
    tags=['gold', 'aggregates', 'care', 'e07']
) }}
-- Care tickets opened per day x channel x category x priority x region, with SLA outcome.
-- Ported from x_e07_ticket_open_daily (PIPE_X.SP_X_E07_TICKET_OPEN_DAILY).
-- Grain   : opened_date x channel x category x priority x region_name
-- Sources : slv_ticket, int_subscriber_profile, sla_policy (seed), dim_date
with t as (
    select
        tk.ticket_id, tk.subscription_id, tk.opened_date, tk.opened_at_local, tk.channel, tk.category, tk.priority,
        tk.is_open, tk.resolution_hours,
        sla.resolution_sla_hours,
        iff(tk.resolution_hours is null, null, tk.resolution_hours <= sla.resolution_sla_hours) as is_within_sla,
        coalesce(p.home_region_name, 'UNKNOWN')             as region_name,
        coalesce(p.churn_band, 'UNSCORED')                  as churn_band,
        p.is_mvno_line
    from {{ ref('slv_ticket') }} tk
    left join {{ ref('sla_policy') }} sla            on sla.priority = tk.priority
    left join {{ ref('int_subscriber_profile') }} p   on p.subscription_id = tk.subscription_id
    where tk.opened_date >= {{ f1_lower_bound('opened_date', 7) }}
),
daily as (
    select
        opened_date, channel, category, priority, region_name,
        count(*)                                            as tickets,
        approx_count_distinct(subscription_id)              as subscribers_approx,
        count_if(is_open)                                   as still_open,
        count_if(is_within_sla)                             as resolved_within_sla,
        count_if(not is_within_sla)                         as resolved_breached,
        round(avg(resolution_hours), 2)                     as avg_resolution_hours,
        median(resolution_hours)                            as median_resolution_hours,
        count_if(churn_band = 'HIGH')                       as tickets_high_churn_lines,
        count_if(is_mvno_line)                              as tickets_mvno_lines,
        count_if(hour(opened_at_local) not between 8 and 19) as tickets_after_hours
    from t
    group by 1, 2, 3, 4, 5
)
select
    d.*,
    dd.day_name,
    dd.is_weekend,
    round(div0(d.resolved_within_sla, d.resolved_within_sla + d.resolved_breached) * 100, 2) as sla_attainment_pct,
    round(ratio_to_report(d.tickets) over (partition by d.opened_date), 6)                  as share_of_day_tickets,
    sum(d.tickets) over (partition by d.channel, d.category, d.priority, d.region_name order by d.opened_date
                         rows between 6 preceding and current row)                          as tickets_7d,
    current_timestamp()                                     as loaded_at
from daily d
left join {{ ref('dim_date') }} dd on dd.date_day = d.opened_date
qualify d.opened_date >= {{ f1_lower_bound('opened_date') }}
