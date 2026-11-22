{#- ===========================================================================
  Helpers for the insight pipelines P08-P11.
=========================================================================== -#}

{#- Latest value of a DATE column minus N days, resolved at run time to a literal so the
    window predicate prunes micro-partitions of large, date-clustered inputs (a scalar
    subquery in a WHERE clause would scan full history). -#}
{% macro p_max_date_literal(rel, col, minus_days=0) -%}
    {%- set exists = execute and adapter.get_relation(database=rel.database, schema=rel.schema, identifier=rel.identifier) is not none -%}
    {%- if exists -%}
        {%- set v = run_query("select to_varchar(dateadd('day', -" ~ minus_days ~ ", max(" ~ col ~ ")), 'YYYY-MM-DD') from " ~ rel).columns[0].values()[0] -%}
        {%- if v -%}'{{ v }}'::date{%- else -%}current_date(){%- endif -%}
    {%- else -%}current_date(){%- endif -%}
{%- endmacro %}

{#- Tenure band used to stratify campaign audiences and control groups (P11). -#}
{% macro p_tenure_band(col) -%}
    case when {{ col }} < 12 then '00-11' when {{ col }} < 24 then '12-23' when {{ col }} < 48 then '24-47' else '48+' end
{%- endmacro %}

{#- Logistic squashing for heuristic propensity scores (P10). -#}
{% macro p_sigmoid(z) -%}
    round(1 / (1 + exp(-({{ z }}))), 4)
{%- endmacro %}
