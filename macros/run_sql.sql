{% macro run_sql(sql) %}
  {% do run_query(sql) %}
  {{ log("Ran: " ~ sql, info=True) }}
{% endmacro %}
