{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_date',
    on_schema_change='append_new_columns',
    cluster_by=['event_date'],
    tags=['gold', 'analytics', 'usage', 'mediation', 'm21']
) }}
-- Mediation xDR signalling events: the events JSON array of every parsed session flattened to one
-- row per event (LATERAL FLATTEN), with inter-event gaps, serving cell / region and subscriber plan.
-- Ported from x_m21_xdr_events_flatten_daily (PIPE_X.SP_X_M21_XDR_EVENTS_FLATTEN_DAILY).
-- Grain: record_id x event_pos.
-- Sources: slv_xdr_session, slv_cell_site, int_subscriber_profile.
with ev as (
    select
        x.record_id,
        x.event_date,
        x.source_system,
        x.record_type,
        x.subscription_id,
        x.cell_id,
        x.rat,
        x.session_start_local,
        x.duration_ms,
        x.charge_php,
        x.is_partial,
        e.index                                             as event_pos,
        upper(e.value:t::string)                            as event_type,
        e.value:ms::number                                  as event_offset_ms
    from {{ ref('slv_xdr_session') }} x,
         lateral flatten(input => x.events, outer => true) e
    where {{ incremental_date_filter('x.event_date', 'event_date') }}
)
select
    ev.record_id,
    ev.event_pos,
    ev.event_date,
    ev.source_system,
    ev.record_type,
    ev.subscription_id,
    ev.cell_id,
    coalesce(c.region_name, 'UNKNOWN')                      as serving_region_name,
    ev.rat,
    p.plan_id,
    p.device_manufacturer,
    ev.event_type,
    ev.event_offset_ms,
    dateadd('millisecond', ev.event_offset_ms, ev.session_start_local) as event_ts_local,
    ev.event_offset_ms - lag(ev.event_offset_ms) over (partition by ev.record_id order by ev.event_pos) as ms_since_prev_event,
    ev.event_type = 'HANDOVER'                              as is_handover,
    count_if(ev.event_type = 'HANDOVER') over (partition by ev.record_id) as handovers_in_session,
    ev.event_offset_ms > ev.duration_ms                     as is_event_after_session_end,
    ev.is_partial,
    ev.charge_php,
    current_timestamp()                                     as loaded_at
from ev
left join {{ ref('slv_cell_site') }} c          on c.cell_id = ev.cell_id
left join {{ ref('int_subscriber_profile') }} p on p.subscription_id = ev.subscription_id
