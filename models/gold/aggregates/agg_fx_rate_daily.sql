{{ config(
    materialized='table',
    cluster_by=['rate_date'],
    tags=['gold', 'aggregates', 'ref', 'finance', 'fx', 'e26']
) }}
-- FX rate to PHP for every calendar day x currency (rates carried forward over gaps with an
-- ASOF JOIN), with day-over-day move and 30-day volatility.
-- Ported from x_e26_fx_rate_daily (PIPE_X.SP_X_E26_FX_RATE_DAILY).
-- Grain   : rate_date x from_currency x to_currency
-- Sources : dim_date (spine), slv_fx_rate, slv_roaming_usage (USD exposure that day)
with days as (
    select date_day as rate_date
    from {{ ref('dim_date') }}
    where date_day between dateadd('day', -30, '{{ var("history_start_date") }}'::date)
                       and (select max(valid_from) from {{ ref('slv_fx_rate') }})
),
pairs as (
    select distinct from_currency, to_currency from {{ ref('slv_fx_rate') }}
),
spine as (
    select d.rate_date, p.from_currency, p.to_currency from days d cross join pairs p
),
rate_asof as (
    select s.rate_date, s.from_currency, s.to_currency, f.rate, f.valid_from, f.rate_source
    from spine s
    asof join {{ ref('slv_fx_rate') }} f
        match_condition (s.rate_date >= f.valid_from)
        on s.from_currency = f.from_currency and s.to_currency = f.to_currency
),
exposure as (
    select event_date as rate_date, sum(charge_usd) as roaming_charge_usd
    from {{ ref('slv_roaming_usage') }}
    group by 1
),
calc as (
    select
        a.*,
        a.valid_from < a.rate_date                          as is_carried_forward,
        lag(a.rate) over (partition by a.from_currency, a.to_currency order by a.rate_date) as prev_rate,
        avg(a.rate) over (partition by a.from_currency, a.to_currency order by a.rate_date rows between 6 preceding and current row)  as rate_7d_avg,
        stddev_samp(a.rate) over (partition by a.from_currency, a.to_currency order by a.rate_date rows between 29 preceding and current row) as rate_30d_stddev,
        min(a.rate) over (partition by a.from_currency, a.to_currency order by a.rate_date rows between 29 preceding and current row) as rate_30d_min,
        max(a.rate) over (partition by a.from_currency, a.to_currency order by a.rate_date rows between 29 preceding and current row) as rate_30d_max
    from rate_asof a
)
select
    c.rate_date, c.from_currency, c.to_currency, c.rate, c.rate_source, c.valid_from as rate_published_on,
    c.is_carried_forward,
    round(div0(c.rate - c.prev_rate, c.prev_rate) * 100, 4) as rate_dod_pct,
    round(c.rate_7d_avg, 6)                                 as rate_7d_avg,
    round(div0(c.rate_30d_stddev, c.rate_7d_avg) * 100, 4)  as volatility_30d_pct,
    c.rate_30d_min, c.rate_30d_max,
    iff(c.from_currency = 'USD', e.roaming_charge_usd, null) as roaming_charge_usd,
    iff(c.from_currency = 'USD', round(e.roaming_charge_usd * c.rate, 2), null) as roaming_exposure_php,
    current_timestamp()                                     as loaded_at
from calc c
left join exposure e on e.rate_date = c.rate_date
where c.rate_date >= '{{ var("history_start_date") }}'::date
  and c.rate is not null
