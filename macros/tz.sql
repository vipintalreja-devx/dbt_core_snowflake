{#- ===========================================================================
  Timezone standardisation. Silver stores every event twice:
    <x>_ts_utc   TIMESTAMP_NTZ in UTC          (machine time, for joins/dedup)
    <x>_ts_local TIMESTAMP_NTZ in var(local_tz) (business time)
    event_date   DATE of the local business day (partition/report key)
  Bronze stores timestamps three different ways, so each source column is
  converted with the macro that matches how it was written:
    tz_to_utc       TIMESTAMP_TZ / TIMESTAMP_LTZ (offset is carried in the value)
    utc_ntz         TIMESTAMP_NTZ already in UTC   (CDR_DATA, CELL_PERFORMANCE_15MIN)
    local_ntz_to_utc TIMESTAMP_NTZ in Manila time  (RECHARGE, WORK_ORDER, STATUS_HISTORY ...)
=========================================================================== -#}
{% macro tz_to_utc(col) -%}
    convert_timezone('UTC', {{ col }})::timestamp_ntz
{%- endmacro %}

{% macro utc_ntz(col) -%}
    {{ col }}::timestamp_ntz
{%- endmacro %}

{% macro local_ntz_to_utc(col) -%}
    convert_timezone('{{ var("local_tz") }}', 'UTC', {{ col }}::timestamp_ntz)
{%- endmacro %}

{% macro utc_to_local(utc_col) -%}
    convert_timezone('UTC', '{{ var("local_tz") }}', {{ utc_col }})
{%- endmacro %}
