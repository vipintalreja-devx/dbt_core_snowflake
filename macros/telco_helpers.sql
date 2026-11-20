{#- Normalise Philippine MSISDNs to E.164 without '+': 639XXXXXXXXX -#}
{% macro normalize_msisdn(col) -%}
    regexp_replace(regexp_replace({{ col }}, '[^0-9+]', ''), '^(\\+?63|0)?(9[0-9]{9})$', '63\\2')
{%- endmacro %}

{#- Mask an MSISDN for analyst-facing gold tables: 63917*****33 -#}
{% macro mask_msisdn(col) -%}
    case when {{ col }} is null then null
         else left({{ col }}, 5) || repeat('*', greatest(length({{ col }}) - 7, 0)) || right({{ col }}, 2) end
{%- endmacro %}

{#- Single governed churn-risk banding -#}
{% macro churn_band(prob_col) -%}
    case when {{ prob_col }} >= {{ var('churn_high_threshold') }} then 'HIGH'
         when {{ prob_col }} >= {{ var('churn_medium_threshold') }} then 'MEDIUM'
         when {{ prob_col }} is not null then 'LOW' end
{%- endmacro %}

{#- Bytes -> MB (binary) -#}
{% macro bytes_to_mb(col) -%}
    ({{ col }} / 1048576)
{%- endmacro %}
