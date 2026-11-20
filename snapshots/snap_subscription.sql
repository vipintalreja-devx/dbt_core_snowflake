{% snapshot snap_subscription %}
{{ config(
    unique_key='subscription_id',
    strategy='check',
    check_cols=['plan_id', 'status', 'home_cell_id', 'device_id', 'partner_brand_id', 'monthly_charges', 'is_deleted'],
    invalidate_hard_deletes=True
) }}
-- SCD2 history of the line's commercial attributes (plan changes, migrations,
-- device swaps, MVNO moves). Run daily: `dbt snapshot`.
select
    subscription_id, account_id, plan_id, upper(status) as status, home_cell_id, device_id,
    partner_brand_id, monthly_charges, coalesce(is_deleted, false) as is_deleted
from {{ source('core', 'subscription') }}
{% endsnapshot %}
