{#- ===========================================================================
  F3 helpers for the advanced (adv_*) models ported from telco_v2_core H05-H12.
  The originals took a single run_month / run_date parameter; the ports process
  every period in the reporting window instead.
=========================================================================== -#}

{#- Lower bound (DATE) of the months to (re)build. Full refresh: first month of
    var('history_start_date'). Incremental: the last loaded month minus one, so the
    previous month is restated together with the current one. -#}
{% macro f3_month_lower_bound(month_col) -%}
    {%- if is_incremental() -%}
        (select coalesce(dateadd('month', -1, max({{ month_col }})), date_trunc('month', '{{ var("history_start_date") }}'::date)) from {{ this }})
    {%- else -%}
        date_trunc('month', '{{ var("history_start_date") }}'::date)
    {%- endif -%}
{%- endmacro %}

{#- Lower bound (DATE) of the days to (re)build for daily advanced models.
    Full refresh: the last var('adv_daily_days', 7) days present in the given silver
    relation/date column (heavy per-event models are kept to a rolling window on an
    X-Small warehouse). Incremental: last loaded day minus var('lookback_days'). -#}
{% macro f3_day_lower_bound(day_col, silver_rel, silver_day_col) -%}
    {%- if is_incremental() -%}
        (select coalesce(dateadd('day', -{{ var('lookback_days') }}, max({{ day_col }})), '{{ var("history_start_date") }}'::date) from {{ this }})
    {%- else -%}
        (select dateadd('day', -{{ var('adv_daily_days', 7) - 1 }}, max({{ silver_day_col }})) from {{ silver_rel }})
    {%- endif -%}
{%- endmacro %}

{#- Last complete reporting month: the month of the latest BSS status event. -#}
{% macro f3_as_of_month() -%}
    (select date_trunc('month', max(valid_from_date)) from {{ ref('slv_subscription_status_history') }})
{%- endmacro %}
