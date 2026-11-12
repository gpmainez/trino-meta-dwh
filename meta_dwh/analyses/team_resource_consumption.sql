-- Question 2: How many resources did team Y consume during a given period?
-- Approach A: Query-grain verification from fact_query_execution (auditable truth)
select
    team_name,
    department,
    count(query_id) as total_queries_executed,
    round(sum(cpu_time_ms) / 1000.0, 2) as total_cpu_seconds,
    round(sum(cpu_time_ms) / (1000.0 * 60.0 * 60.0), 3) as total_cpu_hours,
    round(sum(bytes_scanned) / (1024.0 * 1024.0 * 1024.0), 2) as total_gb_scanned,
    round(avg(duration_ms) / 1000.0, 2) as avg_query_duration_seconds,
    round(max(peak_memory_bytes) / (1024.0 * 1024.0 * 1024.0), 2) as max_peak_memory_gb
from {{ ref('fact_query_execution') }}
where team_name = 'Marketing Analytics'
  and execution_date between date '2026-11-01' and date '2026-11-30'
group by
    team_name,
    department;

-- Approach B: Sub-second FinOps Rollup from mart_daily_team_usage (Dashboards & Reports)
select
    team_name,
    department,
    sum(total_queries) as total_queries_executed,
    round(sum(total_cpu_hours), 2) as total_cpu_hours,
    round(sum(total_gb_scanned), 2) as total_gb_scanned,
    round(sum(total_tb_scanned), 3) as total_tb_scanned,
    round(avg(avg_query_duration_seconds), 2) as avg_query_duration_seconds,
    round(max(max_peak_memory_gb), 2) as max_peak_memory_gb
from {{ ref('mart_daily_team_usage') }}
where team_name = 'Marketing Analytics'
  and usage_date between date '2026-11-01' and date '2026-11-30'
group by
    team_name,
    department;
