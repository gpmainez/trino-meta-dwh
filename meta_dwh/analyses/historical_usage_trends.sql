-- Historical Perspective: Tracking usage trends week-over-week per team and table
-- Business context: Detecting cost anomalies, query growth, and storage/compute scaling needs.

select
    date_trunc('week', execution_date) as execution_week,
    team_name,
    count(distinct query_id) as weekly_queries_count,
    count(distinct user_key) as active_users_count,
    round(sum(cpu_time_ms) / (1000.0 * 60.0 * 60.0), 2) as weekly_cpu_hours,
    round(sum(bytes_scanned) / (1024.0 * 1024.0 * 1024.0 * 1024.0), 3) as weekly_tb_scanned,
    -- Week-over-week CPU growth rate
    round(
        (sum(cpu_time_ms) - lag(sum(cpu_time_ms)) over (partition by team_name order by date_trunc('week', execution_date)))
        * 100.0 / nullif(lag(sum(cpu_time_ms)) over (partition by team_name order by date_trunc('week', execution_date)), 0),
        2
    ) as wow_cpu_growth_pct
from {{ ref('fact_query_execution') }}
group by
    date_trunc('week', execution_date),
    team_name
order by
    execution_week desc,
    weekly_cpu_hours desc;
