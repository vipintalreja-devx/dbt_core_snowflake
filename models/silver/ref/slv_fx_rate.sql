{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'ref'],
    transient=true
) }}
-- Daily FX to PHP with an explicit validity interval for as-of joins.
select
    from_currency,
    to_currency,
    rate_date                                               as valid_from,
    coalesce(lead(rate_date) over (partition by from_currency, to_currency order by rate_date),
             '9999-12-31'::date)                            as valid_to,
    rate::number(18, 6)                                     as rate,
    source                                                  as rate_source
from {{ source('ref', 'currency_rate') }}
qualify row_number() over (partition by from_currency, to_currency, rate_date order by loaded_at desc) = 1
