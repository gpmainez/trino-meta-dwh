-- Question 2: How many resources did team Y consume during a given period?
-- Business context: Team FinOps chargeback and capacity planning.
-- Uses fact_query_execution (query-grain) to guarantee exact cluster CPU and I/O consumption without double-counting.

select
    team_name,
    department,
    count(query_id) as total_queries_executed,
    round(sum(cpu_time_ms) / 1000.0, 2) as total_cpu_seconds,
    round(sum(cpu_time_ms) / (1000.0 * 60.0 * 60.0), 2) as total_cpu_hours,
    round(sum(bytes_scanned) / (1024.0 * 1024.0 * 1024.0), 2) as total_gb_scanned,
    round(avg(duration_ms) / 1000.0, 2) as avg_query_duration_seconds,
    round(max(peak_memory_bytes) / (1024.0 * 1024.0 * 1024.0), 2) as max_peak_memory_gb
from {{ ref('fact_query_execution') }}
where team_name = 'Marketing Analytics' -- replace with target team Y
  and execution_date between date '2026-11-01' and date '2026-11-30' -- target period
group by
    team_name,
    department;
