{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'marts', 'kpi'],
    transient=false,
    copy_grants=true
) }}
select
    opened_date,
    category,
    priority,
    channel,
    count(*)                                                as tickets_opened,
    count_if(not is_open)                                   as tickets_resolved,
    count_if(is_open)                                       as tickets_still_open,
    count_if(is_within_sla)                                 as resolved_within_sla,
    round(div0(count_if(is_within_sla), count_if(not is_open)) * 100, 2) as sla_attainment_pct,
    round(avg(resolution_hours), 2)                         as avg_resolution_hours,
    round(median(resolution_hours), 2)                      as median_resolution_hours,
    sum(reopen_count)                                       as reopens,
    sum(work_orders)                                        as work_orders,
    round(avg(nps_score), 2)                                as avg_nps_score
from {{ ref('fct_ticket') }}
where opened_date >= '{{ var("history_start_date") }}'::date
group by 1, 2, 3, 4
