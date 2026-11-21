{#- ===========================================================================
  Generic tests on the largest incremental tables check a recent window only.
  A nightly unique/not_null test over 300M+ CDR rows rescans full history; telcos
  test the slice each run touched and run a full pass weekly:
      dbt test --vars '{test_recent_days: 0}'        (0 = no window, full history)
  Tables and their date column: var('test_recent_window'). An explicit `where` on a
  test always wins.
=========================================================================== -#}
{% macro snowflake__get_where_subquery(relation) -%}
    {%- set where = config.get('where', '') -%}
    {%- set days = var('test_recent_days', 7) | int -%}
    {%- set col = var('test_recent_window', {}).get(relation.identifier | lower) -%}
    {%- if not where and col and days > 0 -%}
        {%- set where = col ~ " >= dateadd('day', -" ~ days ~ ", current_date())" -%}
    {%- endif -%}
    {%- if where -%}
        {%- set filtered -%}
            (select * from {{ relation }} where {{ where }}) dbt_subquery
        {%- endset -%}
        {%- do return(filtered) -%}
    {%- else -%}
        {%- do return(relation) -%}
    {%- endif -%}
{%- endmacro %}
