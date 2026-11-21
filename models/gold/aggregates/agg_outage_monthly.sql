{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='detected_month',
    on_schema_change='append_new_columns',
    cluster_by=['detected_month'],
    tags=['gold', 'aggregates', 'network', 'outage', 'e21']
) }}
-- Outage incidents per month x severity x root cause x island group: MTTR, customer impact
-- and SLA-credit exposure. Full history.
-- Ported from x_e21_outage_monthly (PIPE_X.SP_X_E21_OUTAGE_MONTHLY).
-- Grain   : detected_month x severity x root_cause x island_group
-- Sources : fct_outage, dim_region, slv_outage_impact, int_subscriber_profile (MRR of impacted lines)
with impact_mrr as (
    select oi.incident_id,
           sum(p.monthly_charges_php)                       as impacted_mrr_php,
           count_if(p.plan_category = 'POSTPAID')           as impacted_postpaid_lines
    from {{ ref('slv_outage_impact') }} oi
    join {{ ref('int_subscriber_profile') }} p on p.subscription_id = oi.subscription_id
    group by 1
),
o as (
    select
        date_trunc('month', f.event_date)::date             as detected_month,
        f.incident_id, f.severity, coalesce(f.root_cause, 'UNKNOWN') as root_cause,
        coalesce(r.island_group, 'UNKNOWN')                 as island_group,
        f.outage_minutes, f.is_ongoing, f.impacted_lines, f.impacted_line_minutes, f.credit_eligible_lines,
        zeroifnull(m.impacted_mrr_php)                      as impacted_mrr_php,
        zeroifnull(m.impacted_postpaid_lines)               as impacted_postpaid_lines
    from {{ ref('fct_outage') }} f
    left join {{ ref('dim_region') }} r on r.region_id = f.region_id
    left join impact_mrr m            on m.incident_id = f.incident_id
    where {{ f2_month_window("date_trunc('month', f.event_date)::date", 'detected_month') }}
)
select
    detected_month, severity, root_cause, island_group,
    count(*)                                                as incidents,
    round(avg(outage_minutes), 0)                           as avg_restore_minutes,
    median(outage_minutes)                                  as mttr_median_minutes,
    approx_percentile(outage_minutes, 0.9)                  as p90_restore_minutes,
    count_if(is_ongoing)                                    as still_open,
    sum(impacted_lines)                                     as impacted_lines,
    sum(impacted_line_minutes)                              as impacted_line_minutes,
    sum(credit_eligible_lines)                              as credit_eligible_lines,
    sum(impacted_postpaid_lines)                            as impacted_postpaid_lines,
    round(sum(impacted_mrr_php), 2)                         as impacted_mrr_php,
    -- pro-rata credit exposure: MRR x share of a 30-day month the line was down
    round(sum(impacted_mrr_php * div0(outage_minutes, 43200)), 2) as est_credit_exposure_php,
    round(ratio_to_report(count(*)) over (partition by detected_month), 6) as share_of_month_incidents,
    current_timestamp()                                     as loaded_at
from o
group by 1, 2, 3, 4
