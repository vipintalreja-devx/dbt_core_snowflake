{% snapshot snap_plan_catalog %}
{{ config(
    unique_key='plan_id',
    strategy='check',
    check_cols=['plan_name', 'contract_type', 'internet_service', 'base_price', 'effective_to']
) }}
-- Price-book history: every tariff change is retained for price/ARPU analysis.
select plan_id, plan_name, contract_type, internet_service, base_price, effective_from, effective_to
from {{ source('ref', 'plan_catalog') }}
{% endsnapshot %}
