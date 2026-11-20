{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'billing'],
    transient=true,
    cluster_by=['invoice_id']
) }}
select
    invoice_id,
    line_no,
    upper(line_type)                                        as line_type,
    description,
    amount::number(18, 2)                                   as line_amount_php,
    amount_precise                                          as line_amount_precise_php
from {{ source('billing', 'invoice_line') }}
qualify row_number() over (partition by invoice_id, line_no order by amount_precise desc) = 1
