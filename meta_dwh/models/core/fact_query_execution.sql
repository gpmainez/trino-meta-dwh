{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'query_id',
    properties = {
        'format': "'ICEBERG'",
        'partitioning': "ARRAY['day(execution_date)']"
    }
) }}

with staging_events as (
    select * from {{ ref('stg_raw_trino_events') }}
    {% if is_incremental() %}
        -- 3-day lookback window prevents late-arriving telemetry and long-running queries from being dropped
        where execution_date >= (select coalesce(max(execution_date) - interval '3' day, date '1970-01-01') from {{ this }})
    {% endif %}
),

users as (
    select * from {{ ref('dim_user') }}
),

final as (
    select
        events.query_id,
        coalesce(u.user_key, '__UNKNOWN_USER__') as user_key,
        events.user_name,
        events.query_state,
        events.is_successful,
        coalesce(u.team_name, 'Unassigned / Service Account') as team_name,
        coalesce(u.department, 'Unassigned Department') as department,
        coalesce(u.cost_center, 'CC-UNASSIGNED') as cost_center,
        events.execution_date,
        events.start_time,
        events.completion_time,
        events.wall_time_ms as duration_ms,
        events.cpu_time_ms,
        events.bytes_scanned,
        events.peak_memory_bytes,
        events.tables_queried_count,
        current_timestamp as dwh_inserted_at
    from staging_events as events
    left join users as u
        on events.user_name = u.user_name
       and events.start_time >= u.valid_from
       and events.start_time <= u.valid_to
)

select * from final
