{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'mart_id',
    properties = {
        'format': "'ICEBERG'",
        'partitioning': "ARRAY['day(usage_date)']"
    }
) }}

with daily_table_access as (
    select
        execution_date as usage_date,
        table_key,
        full_table_name,
        catalog_name,
        schema_name,
        table_name,
        count(distinct query_id) as total_queries_count,
        count(distinct user_key) as exact_distinct_users_count,
        -- Native Trino HyperLogLog Sketch for non-additive distinct users across multi-day rollups
        cast(approx_set(user_key) as varbinary) as hll_users_sketch,
        sum(allocated_cpu_time_ms) as total_allocated_cpu_time_ms,
        sum(allocated_bytes_scanned) as total_allocated_bytes_scanned,
        sum(allocated_peak_memory_bytes) as total_allocated_peak_memory_bytes
    from {{ ref('fact_table_usage') }}
    {% if is_incremental() %}
        where execution_date >= (select coalesce(max(usage_date) - interval '3' day, date '1970-01-01') from {{ this }})
    {% endif %}
    group by
        execution_date,
        table_key,
        full_table_name,
        catalog_name,
        schema_name,
        table_name
),

final as (
    select
        {{ surrogate_key(['usage_date', 'table_key']) }} as mart_id,
        usage_date,
        table_key,
        full_table_name,
        catalog_name,
        schema_name,
        table_name,
        total_queries_count,
        exact_distinct_users_count,
        hll_users_sketch,
        round(total_allocated_cpu_time_ms / 1000.0, 2) as allocated_cpu_seconds,
        round(total_allocated_cpu_time_ms / (1000.0 * 60.0 * 60.0), 3) as allocated_cpu_hours,
        round(total_allocated_bytes_scanned / (1024.0 * 1024.0 * 1024.0), 3) as allocated_gb_scanned,
        round(total_allocated_peak_memory_bytes / (1024.0 * 1024.0 * 1024.0), 2) as allocated_peak_memory_gb,
        current_timestamp as dwh_updated_at
    from daily_table_access
)

select * from final
