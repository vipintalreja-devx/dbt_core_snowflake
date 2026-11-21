{#- Month-grain incremental window (fork F2 / gold.analytics).
    full refresh : months from date_trunc('month', var('history_start_date'))
    incremental  : re-process the latest loaded month and the one before it
                   (late revenue / late CDRs / restated invoices). -#}
{% macro f2_month_window(month_expr, target_col='month_start') -%}
    {%- if is_incremental() -%}
        {{ month_expr }} >= {{ cdc_widen_month("(select coalesce(dateadd('month', -1, max(" ~ target_col ~ ")), date_trunc('month', '" ~ var('history_start_date') ~ "'::date)) from " ~ this ~ ")") }}
    {%- else -%}
        {{ month_expr }} >= date_trunc('month', '{{ var("history_start_date") }}'::date)
    {%- endif -%}
{%- endmacro %}
