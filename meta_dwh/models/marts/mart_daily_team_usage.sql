{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'mart_id',
    properties = {
        'format': "'ICEBERG'",
        'partitioning': "ARRAY['day(usage_date)']"
    }
) }}

with daily_queries as (
    select
        execution_date as usage_date,
        team_name,
        department,
        count(distinct query_id) as total_queries,
        count(distinct user_key) as active_users_count,
        sum(cpu_time_ms) as total_cpu_time_ms,
        sum(bytes_scanned) as total_bytes_scanned,
        sum(duration_ms) as total_duration_ms,
        max(peak_memory_bytes) as max_peak_memory_bytes
    from {{ ref('fact_query_execution') }}
    {% if is_incremental() %}
        where execution_date >= (select coalesce(max(usage_date) - interval '3' day, date '1970-01-01') from {{ this }})
    {% endif %}
    group by
        execution_date,
        team_name,
        department
),

final as (
    select
        {{ surrogate_key(['usage_date', 'team_name', 'department']) }} as mart_id,
        usage_date,
        team_name,
        department,
        total_queries,
        active_users_count,
        round(total_cpu_time_ms / (1000.0 * 60.0 * 60.0), 3) as total_cpu_hours,
        round(total_bytes_scanned / (1024.0 * 1024.0 * 1024.0), 2) as total_gb_scanned,
        round(total_bytes_scanned / (1024.0 * 1024.0 * 1024.0 * 1024.0), 4) as total_tb_scanned,
        round(cast(total_duration_ms as double) / nullif(total_queries, 0) / 1000.0, 2) as avg_query_duration_seconds,
        round(max_peak_memory_bytes / (1024.0 * 1024.0 * 1024.0), 2) as max_peak_memory_gb,
        current_timestamp as dwh_updated_at
    from daily_queries
)

select * from final
