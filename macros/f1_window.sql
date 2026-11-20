{#- ===========================================================================
  F1 helpers for gold aggregates that need trailing context (7-day rolling,
  day-over-day). The source read is widened by `extra_days` so window functions
  are correct for the first day of an incremental batch; the final SELECT then
  keeps only rows >= f1_lower_bound(target_col) (no extra days).
=========================================================================== -#}
{% macro f1_lower_bound(target_date_col, extra_days=0) -%}
    {%- if is_incremental() -%}
        (select dateadd('day', -{{ var('lookback_days') + extra_days }},
                        coalesce(max({{ target_date_col }}), '{{ var("history_start_date") }}'::date))
           from {{ this }})
    {%- else -%}
        dateadd('day', -{{ extra_days }}, '{{ var("history_start_date") }}'::date)
    {%- endif -%}
{%- endmacro %}
