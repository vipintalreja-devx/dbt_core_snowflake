{#- ===========================================================================
  Lineage that catalog-only tools can see.
  dbt builds tables with CREATE TABLE AS SELECT, which records no OBJECT_DEPENDENCIES
  (Snowflake only records view/UDF references). After every run this publishes:
    OPS.LINEAGE_EDGES      declared edges from the dbt graph (sources -> silver -> gold
                           -> snapshots), fully-qualified upper-case object names
    object comments        '<description> | upstream: A, B' on every object built in the run
  OPS.V_ACCESS_LINEAGE (observed, from ACCESS_HISTORY) and OPS.V_LINEAGE_RECONCILE are
  plain views created in ops/sql/06_lineage.sql.
=========================================================================== -#}
{% macro _fqn(rel) -%}{{ (rel or '') | replace('"', '') | upper }}{%- endmacro %}

{% macro _layer(fqn) -%}
    {%- set schema = (fqn.split('.')[1] if fqn.count('.') >= 2 else '') -%}
    {%- if schema.endswith('_SILVER') -%}silver{%- elif schema.endswith('_GOLD') -%}gold
    {%- elif schema.endswith('_SNAPSHOTS') -%}snapshot{%- else -%}bronze{%- endif -%}
{%- endmacro %}

{% macro publish_lineage() %}
    {%- if not execute -%}{{ return('select 1') }}{%- endif -%}
    {%- set rows = [] -%}
    {%- for uid, n in graph.nodes.items() -%}
        {%- if n.resource_type in ('model', 'snapshot', 'seed') and n.relation_name -%}
            {%- set down = _fqn(n.relation_name) -%}
            {%- for p in n.depends_on.nodes -%}
                {%- set up = graph.nodes.get(p) or graph.sources.get(p) -%}
                {%- if up and up.relation_name -%}
                    {%- set upf = _fqn(up.relation_name) -%}
                    {%- do rows.append("('" ~ upf ~ "','" ~ _layer(upf) ~ "','" ~ up.resource_type ~ "','" ~ down ~ "','"
                                       ~ _layer(down) ~ "','" ~ n.resource_type ~ "','" ~ (n.config.materialized or n.resource_type) ~ "','" ~ uid ~ "')") -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}
    create or replace table {{ target.database }}.OPS.LINEAGE_EDGES
      comment = 'Declared lineage from the dbt graph, republished after every dbt run (macros/lineage.sql)' as
    select column1 as upstream_object, column2 as upstream_layer, column3 as upstream_type,
           column4 as downstream_object, column5 as downstream_layer, column6 as downstream_type,
           column7 as materialization, column8 as dbt_unique_id,
           '{{ invocation_id }}' as invocation_id, current_timestamp() as published_at
      from values {{ rows | join(', ') }}
{% endmacro %}

{#- Append 'upstream: ...' to the comment of every object built in this run. -#}
{% macro comment_upstreams(results) %}
    {%- if not execute or not var('lineage_comments', true) -%}{{ return('select 1') }}{%- endif -%}
    {%- for r in results -%}
        {%- set n = r.node -%}
        {%- if r.status == 'success' and n.resource_type in ('model', 'snapshot') and n.relation_name -%}
            {%- set ups = [] -%}
            {%- for p in n.depends_on.nodes -%}
                {%- set up = graph.nodes.get(p) or graph.sources.get(p) -%}
                {%- if up and up.relation_name -%}{%- do ups.append(_fqn(up.relation_name)) -%}{%- endif -%}
            {%- endfor -%}
            {%- set kind = 'view' if n.config.materialized == 'view' else ('dynamic table' if n.config.materialized == 'dynamic_table' else 'table') -%}
            {%- set desc = (n.description or '') | replace("'", "''") | replace('\n', ' ') -%}
            {%- set txt = (desc[:600] ~ ' | upstream: ' ~ (ups | join(', ')))[:4000] -%}
            {%- do run_query("comment on " ~ kind ~ " " ~ n.relation_name ~ " is '" ~ txt ~ "'") -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return('select 1') }}
{% endmacro %}
