{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'facts', 'fact'],
    transient=false,
    copy_grants=true
) }}
-- Grain: outage incident, with customer impact and SLA-credit exposure.
with impact as (
    select incident_id,
           count(*)                                         as impacted_lines,
           sum(impact_minutes)                              as impacted_line_minutes,
           count_if(is_credit_eligible)                     as credit_eligible_lines
    from {{ ref('slv_outage_impact') }}
    group by 1
)
select
    o.incident_id, o.severity, o.region_id, r.region_name, o.event_date,
    o.detected_at_utc, o.restored_at_utc, o.outage_minutes, o.is_ongoing, o.root_cause,
    zeroifnull(i.impacted_lines)                            as impacted_lines,
    zeroifnull(i.impacted_line_minutes)                     as impacted_line_minutes,
    zeroifnull(i.credit_eligible_lines)                     as credit_eligible_lines
from {{ ref('slv_outage_incident') }} o
left join {{ ref('slv_region') }} r on r.region_id = o.region_id
left join impact i on i.incident_id = o.incident_id
