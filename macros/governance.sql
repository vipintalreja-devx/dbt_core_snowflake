{#- ===========================================================================
  Regional row-level security on gold.
    REGION_ID or HOME_REGION_ID column -> GOV.RAP_REGION      (numeric region id)
    otherwise REGION_NAME column        -> GOV.RAP_REGION_NAME (region name)
  Applies to tables, incremental tables and dynamic tables.
  dbt rebuilds tables with CREATE OR REPLACE, which drops attached policies, so the policy
  is re-attached by a post-hook after every build. Incremental tables keep it between runs;
  drop+add keeps the hook idempotent. Two hooks: Snowflake runs one statement per call.
=========================================================================== -#}
{% macro _rap_target() %}
    {%- if not execute or model.resource_type != 'model' -%}{{ return(none) }}{%- endif -%}
    {%- if model.config.materialized not in ('table', 'incremental', 'dynamic_table') -%}{{ return(none) }}{%- endif -%}
    {%- set cols = adapter.get_columns_in_relation(this) | map(attribute='name') | map('lower') | list -%}
    {%- for c in ['region_id', 'home_region_id'] -%}
        {%- if c in cols -%}{{ return(('RAP_REGION', c)) }}{%- endif -%}
    {%- endfor -%}
    {%- if 'region_name' in cols -%}{{ return(('RAP_REGION_NAME', 'region_name')) }}{%- endif -%}
    {{ return(none) }}
{% endmacro %}

{% macro rap_region_drop() -%}
    {%- set t = _rap_target() -%}
    {%- if t -%}
    alter {{ 'dynamic table' if model.config.materialized == 'dynamic_table' else 'table' }} {{ this }} drop all row access policies
    {%- else -%}
    select 'no region column'
    {%- endif -%}
{%- endmacro %}

{% macro rap_region_add() -%}
    {%- set t = _rap_target() -%}
    {%- if t -%}
    alter {{ 'dynamic table' if model.config.materialized == 'dynamic_table' else 'table' }} {{ this }} add row access policy {{ target.database }}.GOV.{{ t[0] }} on ({{ t[1] }})
    {%- else -%}
    select 'no region column'
    {%- endif -%}
{%- endmacro %}

{#- seeds/region_access_map.csv -> GOV.REGION_ACCESS_MAP (version-controlled mapping) -#}
{% macro sync_region_access_map_delete() -%}
    delete from {{ target.database }}.GOV.REGION_ACCESS_MAP m
     where not exists (select 1 from {{ this }} s where s.role_name = m.role_name and s.region_name = m.region_name)
{%- endmacro %}

{% macro sync_region_access_map_insert() -%}
    insert into {{ target.database }}.GOV.REGION_ACCESS_MAP (ROLE_NAME, REGION_NAME)
    select s.role_name, s.region_name from {{ this }} s
     where not exists (select 1 from {{ target.database }}.GOV.REGION_ACCESS_MAP m
                        where m.role_name = s.role_name and m.region_name = s.region_name)
{%- endmacro %}
