-- Question: Which queries exhibit low parallelization efficiency, indicating sequential bottlenecks?
-- Business Context: Identifies inefficient SQL patterns (e.g. unpartitioned cartesian joins, lack of broadcast)
-- Parallelism Factor = total_cpu_time / wall_clock_time. 
-- A factor close to 1.0 means the query executed sequentially on a single core despite consuming significant cluster resources.

select
    events.query_id,
    events.user_name,
    events.team_name,
    events.department,
    events.execution_date,
    round(events.cpu_time_ms / 1000.0, 2) as cpu_seconds,
    round(events.duration_ms / 1000.0, 2) as wall_clock_seconds,
    round(events.bytes_scanned / (1024.0 * 1024.0 * 1024.0), 2) as gb_scanned,
    -- Parallelism Factor: Average number of CPU cores kept busy simultaneously
    round(cast(events.cpu_time_ms as double) / nullif(events.duration_ms, 0), 2) as parallelism_factor,
    case
        when events.duration_ms > 30000 and (cast(events.cpu_time_ms as double) / nullif(events.duration_ms, 0)) < 1.5
            then 'CRITICAL_SEQUENTIAL_BOTTLENECK'
        when (cast(events.cpu_time_ms as double) / nullif(events.duration_ms, 0)) >= 8.0
            then 'EXCELLENT_DISTRIBUTED_SCALE'
        else 'NORMAL_EXECUTION'
    end as finops_efficiency_tier
from {{ ref('fact_query_execution') }} as events
where events.execution_date between date '2026-11-01' and date '2026-11-30'
order by
    cpu_seconds desc
limit 50;
