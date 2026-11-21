{{ config(
    materialized='table',
    cluster_by=['end_month'],
    tags=['gold', 'aggregates', 'core', 'retention', 'e13']
) }}
-- Materialization: table (current-state/small reference): contract end months are forward-looking from the current contract master (600k rows).
-- Contracts reaching their end date per month x contract type x auto-renew x churn band:
-- the renewal pipeline and revenue at risk.
-- Ported from x_e13_contract_expiry_monthly (PIPE_X.SP_X_E13_CONTRACT_EXPIRY_MONTHLY).
-- Grain   : end_month x contract_type x is_auto_renew x churn_band
-- Sources : slv_contract, int_subscriber_profile, slv_plan
with c as (
    select
        date_trunc('month', ct.end_date)::date              as end_month,
        ct.contract_id, ct.subscription_id, ct.contract_type, ct.is_auto_renew, ct.terminated_on, ct.is_terminated,
        ct.commitment_months,
        coalesce(p.churn_band, 'UNSCORED')                  as churn_band,
        p.churn_probability,
        p.monthly_charges_php,
        p.status                                            as line_status,
        pl.is_sellable                                      as plan_still_sellable,
        pl.base_price_php
    from {{ ref('slv_contract') }} ct
    left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = ct.subscription_id
    left join {{ ref('slv_plan') }} pl             on pl.plan_id = p.plan_id
    where ct.end_date is not null
)
select
    end_month, contract_type, is_auto_renew, churn_band,
    count(*)                                                as contracts,
    count_if(is_terminated and terminated_on < end_month)   as terminated_early,
    count_if(line_status in ('ACTIVE', 'SUSPENDED'))        as lines_still_in_base,
    round(avg(churn_probability), 4)                        as avg_churn_probability,
    sum(iff(line_status in ('ACTIVE', 'SUSPENDED'), monthly_charges_php, 0)) as monthly_revenue_at_risk_php,
    sum(iff(line_status in ('ACTIVE', 'SUSPENDED'), monthly_charges_php * 12, 0) * zeroifnull(churn_probability)) as expected_annual_loss_php,
    count_if(not plan_still_sellable)                       as on_retired_plans,
    round(avg(monthly_charges_php - base_price_php), 2)     as avg_charge_vs_list_php,
    round(ratio_to_report(count(*)) over (partition by end_month), 6) as share_of_month_expiries,
    current_timestamp()                                     as loaded_at
from c
group by 1, 2, 3, 4
