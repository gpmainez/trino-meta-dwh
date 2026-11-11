-- Question 1: How many distinct users queried the table X during a given period?
-- Business context: Assess table popularity and identify true user adoption.
-- Utilizes fact_table_usage for table-level granularity and partition pruning on execution_date.

select
    full_table_name,
    count(distinct user_key) as distinct_users_count,
    min(start_time) as period_start,
    max(start_time) as period_end,
    count(query_id) as total_queries_count,
    round(sum(allocated_cpu_time_ms) / 1000.0, 2) as total_allocated_cpu_seconds,
    round(sum(allocated_bytes_scanned) / (1024.0 * 1024.0 * 1024.0), 3) as total_allocated_gb_scanned
from {{ ref('fact_table_usage') }}
where full_table_name = 'iceberg.sales.orders'  -- replace with target table X
  and execution_date between date '2026-11-01' and date '2026-11-07' -- target period
group by
    full_table_name;
