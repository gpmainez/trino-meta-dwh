{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'usage_id',
    properties = {
        'format': "'ICEBERG'",
        'partitioning': "ARRAY['day(execution_date)']"
    }
) }}

with staging_table_events as (
    select * from {{ ref('stg_raw_trino_event_tables') }}
    {% if is_incremental() %}
        -- 3-day lookback window prevents late-arriving telemetry from being dropped
        where execution_date >= (select coalesce(max(execution_date) - interval '3' day, date '1970-01-01') from {{ this }})
    {% endif %}
),

users as (
    select * from {{ ref('dim_user') }}
),

final as (
    select
        {{ surrogate_key(['events.query_id', 'events.table_key']) }} as usage_id,
        events.query_id,
        events.table_key,
        coalesce(u.user_key, '__UNKNOWN_USER__') as user_key,
        events.user_name,
        coalesce(u.team_name, 'Unassigned / Service Account') as team_name,
        coalesce(u.department, 'Unassigned Department') as department,
        events.full_table_name,
        events.catalog_name,
        events.schema_name,
        events.table_name,
        events.execution_date,
        events.start_time,
        events.completion_time,
        events.tables_queried_count,
        events.cost_allocation_factor,
        events.allocated_cpu_time_ms,
        events.allocated_bytes_scanned,
        events.allocated_peak_memory_bytes,
        current_timestamp as dwh_inserted_at
    from staging_table_events as events
    left join users as u
        on events.user_name = u.user_name
       and events.start_time >= u.valid_from
       and events.start_time <= u.valid_to
)

select * from final
