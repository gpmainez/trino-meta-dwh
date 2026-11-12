with source_data as (
    select * from {{ source('trino_source', 'raw_trino_events') }}
),

renamed_and_filtered as (
    select
        query_id,
        lower(trim(user_name)) as user_name,
        query_state,
        case when query_state = 'FINISHED' then true else false end as is_successful,
        cast(cpu_time_ms as bigint) as cpu_time_ms,
        cast(wall_time_ms as bigint) as wall_time_ms,
        cast(bytes_scanned as bigint) as bytes_scanned,
        cast(peak_memory_bytes as bigint) as peak_memory_bytes,
        cast(start_time as timestamp(6) with time zone) as start_time,
        cast(completion_time as timestamp(6) with time zone) as completion_time,
        cast(date_trunc('day', start_time) as date) as execution_date,
        coalesce(tables, array[]) as tables,
        coalesce(cardinality(tables), 0) as tables_queried_count
    from source_data
    where cpu_time_ms is not null and cpu_time_ms >= 0
)

select * from renamed_and_filtered
