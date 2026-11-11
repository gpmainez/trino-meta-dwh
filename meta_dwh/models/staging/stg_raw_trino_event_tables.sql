with base_events as (
    select * from {{ ref('stg_raw_trino_events') }}
    where tables_queried_count > 0
),

unnested_tables as (
    select
        events.query_id,
        events.user_name,
        events.execution_date,
        events.start_time,
        events.completion_time,
        events.cpu_time_ms,
        events.wall_time_ms,
        events.bytes_scanned,
        events.peak_memory_bytes,
        events.tables_queried_count,
        cast(1.0 as double) / cast(events.tables_queried_count as double) as cost_allocation_factor,
        lower(trim(t.raw_table_identifier)) as raw_table_identifier
    from base_events as events
    cross join unnest(events.tables) as t(raw_table_identifier)
),

parsed_identifiers as (
    select
        query_id,
        user_name,
        execution_date,
        start_time,
        completion_time,
        cpu_time_ms,
        wall_time_ms,
        bytes_scanned,
        peak_memory_bytes,
        tables_queried_count,
        cost_allocation_factor,
        raw_table_identifier,

        -- Split multi-part identifier (catalog.schema.table or schema.table or table)
        case
            when cardinality(split(raw_table_identifier, '.')) >= 3 then split(raw_table_identifier, '.')[1]
            else 'default_catalog'
        end as catalog_name,

        case
            when cardinality(split(raw_table_identifier, '.')) >= 3 then split(raw_table_identifier, '.')[2]
            when cardinality(split(raw_table_identifier, '.')) = 2 then split(raw_table_identifier, '.')[1]
            else 'default_schema'
        end as schema_name,

        case
            when cardinality(split(raw_table_identifier, '.')) >= 3 then split(raw_table_identifier, '.')[3]
            when cardinality(split(raw_table_identifier, '.')) = 2 then split(raw_table_identifier, '.')[2]
            else raw_table_identifier
        end as table_name

    from unnested_tables
),

final as (
    select
        query_id,
        user_name,
        execution_date,
        start_time,
        completion_time,
        raw_table_identifier,
        catalog_name,
        schema_name,
        table_name,
        concat_ws('.', catalog_name, schema_name, table_name) as full_table_name,
        {{ surrogate_key(['catalog_name', 'schema_name', 'table_name']) }} as table_key,
        tables_queried_count,
        cost_allocation_factor,
        round(cast(cpu_time_ms as double) * cost_allocation_factor) as allocated_cpu_time_ms,
        round(cast(bytes_scanned as double) * cost_allocation_factor) as allocated_bytes_scanned,
        round(cast(peak_memory_bytes as double) * cost_allocation_factor) as allocated_peak_memory_bytes
    from parsed_identifiers
)

select * from final
