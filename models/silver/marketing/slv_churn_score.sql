{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'marketing'],
    transient=true
) }}
-- Latest churn-model score per line (embedding vector intentionally dropped).
select
    subscription_id,
    score_date,
    churn_probability,
    {{ churn_band('churn_probability') }}                   as churn_band,
    model_version
from {{ source('marketing', 'churn_score') }}
qualify row_number() over (partition by subscription_id order by score_date desc, model_version desc) = 1
