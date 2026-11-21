{#- ===========================================================================
  Change-data-capture (CDC) for the medallion.

  Source tables keep their original shapes. Snowflake CHANGE_TRACKING (a table
  property) lets a silver model read only what changed in bronze since the model's
  last successful run: new rows, updated rows and hard-deleted rows.

  Control tables (TELCO_PROD_XL_V2.OPS, created by the on-run-start hook):
    DBT_MODEL_WATERMARK  model -> run_started_at of its last successful run
    DBT_CHANGED_DATES    event dates touched per silver model per run (incl. deletes)
    DBT_CDC_DELETE_LOG   keys hard-deleted upstream, propagated to gold

  Flow per run
    silver row model : CHANGES(...) AT(watermark)  -> merge (deleted rows arrive as
                       tombstones with _cdc_action = 'DELETE') -> post-hooks log the
                       changed dates + deleted keys, then purge the tombstones
    silver aggregate : recompute only the dates present in CHANGES(...)
    gold incremental : window lower bound = least(old lookback bound,
                       earliest date changed upstream since the gold model's watermark)
  Fallback (first run, full refresh, watermark older than the retention window):
  the original event-date lookback filter.
=========================================================================== -#}

{% macro ops_relation(name) -%}
    {{ target.database }}.OPS.{{ name }}
{%- endmacro %}

{#- run_started_at of the current invocation as TIMESTAMP_LTZ -#}
{% macro dbt_loaded_at() -%}
    '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f") }} +00:00'::timestamp_tz::timestamp_ltz
{%- endmacro %}

{#- SQL scalar: this model's last successful run start (NULL if never). -#}
{% macro model_watermark_sql(model_name=none) -%}
    (select max(last_success_run_started_at) from {{ ops_relation('DBT_MODEL_WATERMARK') }}
      where model_name = '{{ (model_name or this.identifier) | lower }}')
{%- endmacro %}

{#- Watermark resolved at compile time, so it can be used inside AT(TIMESTAMP => ...).
    Returns none unless the model is incremental, has a watermark and the watermark is
    inside the source's change-tracking retention. -#}
{% macro cdc_watermark(retention_hours=150) %}
    {%- if not (execute and is_incremental()) -%}{{ return(none) }}{%- endif -%}
    {%- set r = run_query("select to_varchar(max(last_success_run_started_at), 'YYYY-MM-DD HH24:MI:SS.FF6 TZHTZM'), "
                          ~ "datediff('hour', max(last_success_run_started_at), current_timestamp()) from "
                          ~ ops_relation('DBT_MODEL_WATERMARK') ~ " where model_name = '" ~ (this.identifier | lower) ~ "'") -%}
    {%- set wm, age = r.columns[0].values()[0], r.columns[1].values()[0] -%}
    {%- if wm is none or age is none or age >= retention_hours -%}{{ return(none) }}{%- endif -%}
    {{ return(wm) }}
{% endmacro %}

{#- Row source for a silver CDC model. Adds _cdc_action ('INSERT' | 'DELETE').
    fallback_where: the original event-time filter, used on full refresh / no watermark. -#}
{% macro cdc_source(source_name, table_name, fallback_where=none, retention_hours=150) -%}
    {%- set wm = cdc_watermark(retention_hours) -%}
    {%- set rel = source(source_name, table_name) -%}
    {%- if wm -%}
    (select c.*, c.metadata$action::varchar as _cdc_action
       from {{ rel }} changes(information => default) at(timestamp => '{{ wm }}'::timestamp_tz) c
      where not (c.metadata$action = 'DELETE' and c.metadata$isupdate))
    {%- else -%}
    (select s.*, 'INSERT'::varchar as _cdc_action
       from {{ rel }} s
     {%- if fallback_where %} where {{ fallback_where }}{% endif %})
    {%- endif -%}
{%- endmacro %}

{#- Boolean filter for silver AGGREGATES: recompute only the dates that changed in the
    source. date_expr_src is evaluated on the source columns, col is the same date in
    the model's own query. -#}
{% macro cdc_changed_dates_filter(source_name, table_name, date_expr_src, col, fallback_condition, retention_hours=150) -%}
    {%- set wm = cdc_watermark(retention_hours) -%}
    {%- if wm -%}
    {{ col }} in (select distinct {{ date_expr_src }}
                    from {{ source(source_name, table_name) }} changes(information => default) at(timestamp => '{{ wm }}'::timestamp_tz))
    {%- else -%}
    {{ fallback_condition }}
    {%- endif -%}
{%- endmacro %}

{#- ---------------------------------------------------------------- post-hooks -#}
{#- 1. record which event dates this run changed (upserts and tombstones) -#}
{% macro cdc_log_changed_dates(date_col='event_date', has_action=true) -%}
    insert into {{ ops_relation('DBT_CHANGED_DATES') }} (model_name, event_date, change_type, invocation_id, loaded_at)
    select '{{ this.identifier | lower }}', {{ date_col }},
           {% if has_action %}iff(_cdc_action = 'DELETE', 'DELETE', 'UPSERT'){% else %}'UPSERT'{% endif %},
           '{{ invocation_id }}', current_timestamp()
      from {{ this }}
     where _dbt_loaded_at = {{ dbt_loaded_at() }} and {{ date_col }} is not null
     group by 2, 3
{%- endmacro %}

{#- 2. remember hard-deleted keys so gold can remove them too -#}
{% macro cdc_log_deletes(key_col, date_col='event_date') -%}
    insert into {{ ops_relation('DBT_CDC_DELETE_LOG') }} (model_name, key_value, event_date, invocation_id, deleted_at)
    select '{{ this.identifier | lower }}', {{ key_col }}::varchar, {{ date_col }}, '{{ invocation_id }}', current_timestamp()
      from {{ this }} where _cdc_action = 'DELETE'
{%- endmacro %}

{#- 3. purge tombstones from silver -#}
{% macro cdc_purge_tombstones() -%}
    delete from {{ this }} where _cdc_action = 'DELETE'
{%- endmacro %}

{#- Gold post-hook: drop rows whose upstream silver key was hard-deleted -#}
{% macro cdc_apply_upstream_deletes(key_col, upstream_model, key_type='number') -%}
    delete from {{ this }}
     where {{ key_col }} in (select try_cast(key_value as {{ key_type }}) from {{ ops_relation('DBT_CDC_DELETE_LOG') }}
                              where model_name = '{{ upstream_model | lower }}')
{%- endmacro %}

{#- ---------------------------------------------------------------- gold windows -#}
{#- Earliest date changed in silver since this model's last success (NULL = no info). -#}
{#- Names of the CDC silver models upstream of the current model (transitively, via
    the dbt graph). Only their changes may widen this model's window. -#}
{% macro cdc_upstream_models() %}
    {%- if not execute -%}{{ return([]) }}{%- endif -%}
    {%- set seen = [] -%}
    {%- set frontier = [model.unique_id] -%}
    {%- for depth in range(12) -%}
        {%- set nxt = [] -%}
        {%- for uid in frontier -%}
            {%- set n = graph.nodes.get(uid) -%}
            {%- if n -%}
                {%- for p in n.depends_on.nodes -%}
                    {%- if p not in seen and p.startswith('model.') -%}
                        {%- do seen.append(p) -%}{%- do nxt.append(p) -%}
                    {%- endif -%}
                {%- endfor -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do frontier.clear() -%}{%- do frontier.extend(nxt) -%}
    {%- endfor -%}
    {%- set names = [] -%}
    {%- for uid in seen -%}
        {%- set n = graph.nodes.get(uid) -%}
        {%- if n and 'cdc' in n.tags -%}{%- do names.append((n.alias or n.name) | lower) -%}{%- endif -%}
    {%- endfor -%}
    {{ return(names) }}
{% endmacro %}

{% macro cdc_min_changed_date_sql() -%}
    {%- set ups = cdc_upstream_models() -%}
    {%- if ups | length == 0 -%}
    null::date
    {%- else -%}
    (select min(cd.event_date) from {{ ops_relation('DBT_CHANGED_DATES') }} cd
      where cd.model_name in ('{{ ups | join("', '") }}')
        and cd.loaded_at > {{ model_watermark_sql() }})
    {%- endif -%}
{%- endmacro %}

{#- Widen an incremental lower bound (DATE expression) so it also covers every date
    changed upstream since the last successful run (late rows, deletes, restatements). -#}
{#- Resolve a DATE expression to a literal at run time. A literal bound lets Snowflake
    prune micro-partitions of the (event_date-clustered) inputs; a scalar subquery in the
    WHERE clause does not, and would make every incremental run scan full history. -#}
{% macro _date_literal(expr_sql) %}
    {%- if execute and var('cdc_literal_bounds', true) -%}
        {%- set v = run_query("select to_varchar((" ~ expr_sql ~ ")::date, 'YYYY-MM-DD')").columns[0].values()[0] -%}
        {%- if v is not none -%}{{ return("'" ~ v ~ "'::date") }}{%- endif -%}
    {%- endif -%}
    {{ return(expr_sql) }}
{% endmacro %}

{#- Widen an incremental lower bound (DATE expression) so it also covers every date
    changed upstream since the last successful run (late rows, deletes, restatements). -#}
{% macro cdc_widen(lower_bound_sql) -%}
    {%- if is_incremental() and var('cdc_enabled', true) -%}
    {{ _date_literal("least(" ~ lower_bound_sql ~ ", coalesce(" ~ cdc_min_changed_date_sql() ~ ", " ~ lower_bound_sql ~ "))") }}
    {%- elif is_incremental() -%}
    {{ _date_literal(lower_bound_sql) }}
    {%- else -%}
    {{ lower_bound_sql }}
    {%- endif -%}
{%- endmacro %}

{% macro cdc_widen_month(lower_bound_sql) -%}
    {%- if is_incremental() and var('cdc_enabled', true) -%}
    {{ _date_literal("least(" ~ lower_bound_sql ~ ", coalesce(date_trunc('month', " ~ cdc_min_changed_date_sql() ~ "), " ~ lower_bound_sql ~ "))") }}
    {%- elif is_incremental() -%}
    {{ _date_literal(lower_bound_sql) }}
    {%- else -%}
    {{ lower_bound_sql }}
    {%- endif -%}
{%- endmacro %}

{#- ---------------------------------------------------------------- run hooks -#}
{#- One statement per hook (Snowflake runs one statement per call). -#}
{% macro cdc_create_control_table(name) -%}
    {%- set ddl = {
        'DBT_MODEL_WATERMARK': 'model_name varchar, resource_type varchar, relation_name varchar, last_success_run_started_at timestamp_ltz, last_invocation_id varchar, updated_at timestamp_ltz',
        'DBT_CHANGED_DATES':   'model_name varchar, event_date date, change_type varchar, invocation_id varchar, loaded_at timestamp_ltz',
        'DBT_CDC_DELETE_LOG':  'model_name varchar, key_value varchar, event_date date, invocation_id varchar, deleted_at timestamp_ltz',
    } -%}
    create table if not exists {{ ops_relation(name) }} ({{ ddl[name] }})
{%- endmacro %}

{% macro cdc_record_watermarks(results) %}
    {%- if not execute -%}{{ return('select 1') }}{%- endif -%}
    {%- set rows = [] -%}
    {%- for r in results -%}
        {%- if r.status in ('success', 'pass') and r.node.resource_type in ('model', 'snapshot', 'seed') -%}
            {%- do rows.append("select '" ~ (r.node.alias or r.node.name) | lower ~ "' as m, '" ~ r.node.resource_type ~ "' as t, '"
                               ~ (r.node.relation_name or '') | replace('"', '') ~ "' as rel") -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if rows | length == 0 -%}{{ return('select 1') }}{%- endif -%}
    merge into {{ ops_relation('DBT_MODEL_WATERMARK') }} w
    using ({{ rows | join(' union all ') }}) s on w.model_name = s.m
    when matched then update set last_success_run_started_at = {{ dbt_loaded_at() }}, last_invocation_id = '{{ invocation_id }}',
                                 relation_name = s.rel, updated_at = current_timestamp()
    when not matched then insert (model_name, resource_type, relation_name, last_success_run_started_at, last_invocation_id, updated_at)
         values (s.m, s.t, s.rel, {{ dbt_loaded_at() }}, '{{ invocation_id }}', current_timestamp())
{% endmacro %}

{#- Full refresh keeps the history window; CDC runs accept any date (late rows). -#}
{% macro full_refresh_floor(col) -%}
    {%- if is_incremental() -%} true {%- else -%} {{ col }} >= '{{ var("history_start_date") }}'::date {%- endif -%}
{%- endmacro %}

{#- Subquery of the dates whose source rows changed since this model's watermark, or
    none when not in CDC mode (caller then applies its fallback window). -#}
{% macro cdc_source_changed_dates(source_name, table_name, date_expr_src, retention_hours=150) -%}
    {%- set wm = cdc_watermark(retention_hours) -%}
    {%- if wm -%}
        {{ return("(select distinct " ~ date_expr_src ~ " from " ~ source(source_name, table_name)
                  ~ " changes(information => default) at(timestamp => '" ~ wm ~ "'::timestamp_tz))") }}
    {%- else -%}
        {{ return(none) }}
    {%- endif -%}
{%- endmacro %}

{#- Keys of rows in a CDC silver model changed since THIS model's last success.
    Used by gold row-level facts (merge on the key) to recompute only affected entities,
    e.g. tickets whose status history, work orders or NPS changed. Returns a subquery;
    on full refresh it returns all keys. -#}
{% macro cdc_changed_keys(silver_ref, key_col, out_col=none) -%}
    (select distinct {{ key_col }}{% if out_col %} as {{ out_col }}{% endif %} from {{ silver_ref }}
     {%- if is_incremental() %}
      where _dbt_loaded_at > coalesce({{ model_watermark_sql() }}, '1900-01-01'::timestamp_ltz)
     {%- endif %})
{%- endmacro %}

{#- Filter for gold row-level models reading ONE CDC silver model directly. -#}
{% macro cdc_rows_since_watermark(alias='') -%}
    {%- if is_incremental() -%}
    {{ alias ~ '.' if alias }}_dbt_loaded_at > coalesce({{ model_watermark_sql() }}, '1900-01-01'::timestamp_ltz)
    {%- else -%}
    true
    {%- endif -%}
{%- endmacro %}
