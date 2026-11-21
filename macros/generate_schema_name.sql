{#-
  Every target writes <target.schema>_<custom>, e.g. DBT_TELCO + silver -> DBT_TELCO_SILVER.
  The local dev target and the Snowflake-native prod target (dbt Projects on Snowflake,
  OPS.TELCO_DBT) both use schema DBT_TELCO, so they build the same DBT_TELCO_* schemas;
  a CI target can point target.schema elsewhere (e.g. DBT_CI) to build in isolation.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
