{#- ===========================================================================
  Incremental window on a DATE column (business day).
  * full refresh / first run : load from var('history_start_date')
  * incremental              : re-load the last var('lookback_days') days that
                               already exist in the target (late-arriving data)
  Used together with incremental_strategy='delete+insert' and
  unique_key=<date col> (aggregates) or the natural event key (events).
=========================================================================== -#}
{% macro incremental_date_filter(date_col, target_date_col=none) -%}
    {%- set tcol = target_date_col or date_col -%}
    {%- if is_incremental() -%}
        {{ date_col }} >= (select coalesce(dateadd('day', -{{ var('lookback_days') }}, max({{ tcol }})), '{{ var("history_start_date") }}'::date) from {{ this }})
    {%- else -%}
        {{ date_col }} >= '{{ var("history_start_date") }}'::date
    {%- endif -%}
{%- endmacro %}

{#- Same window expressed as a UTC timestamp lower bound, for pruning raw
    event tables on their native timestamp column before conversion.
    One extra day of slack covers the UTC/local offset. -#}
{% macro incremental_ts_lower_bound(target_date_col='event_date') -%}
    {%- if is_incremental() -%}
        (select dateadd('day', -{{ var('lookback_days') + 1 }}, coalesce(max({{ target_date_col }}), '{{ var("history_start_date") }}'::date))::timestamp_ntz from {{ this }})
    {%- else -%}
        dateadd('day', -1, '{{ var("history_start_date") }}'::date)::timestamp_ntz
    {%- endif -%}
{%- endmacro %}
