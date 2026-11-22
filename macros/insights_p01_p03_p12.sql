{#- ===========================================================================
  Helpers for insight pipelines P01, P02, P03, P12.
=========================================================================== -#}

{#- Lower bound (DATE) for subscriber x day insight models that keep a rolling window.
    Full refresh: var('insights_daily_days') days back from the latest date of the input.
    Incremental : the CDC-aware f1_lower_bound of this model (late rows widen it). -#}
{% macro ins_daily_lower_bound(target_col, input_rel, input_col, extra_days=0) -%}
    {%- if is_incremental() -%}
        {{ f1_lower_bound(target_col, extra_days) }}
    {%- else -%}
        {#- subquery, not a literal: on a first build the input may not exist at compile time -#}
        (select dateadd('day', -{{ var('insights_daily_days') | int + extra_days }}, max({{ input_col }})) from {{ input_rel }})
    {%- endif -%}
{%- endmacro %}

{#- Post-hook: keep only the rolling window (relative to the latest loaded date). -#}
{% macro ins_retention(date_col) -%}
    delete from {{ this }}
     where {{ date_col }} < (select dateadd('day', -{{ var('insights_daily_days') }}, max({{ date_col }})) from {{ this }})
{%- endmacro %}
