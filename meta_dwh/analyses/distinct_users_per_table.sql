-- Question 1: How many distinct users queried the table X during a given period?
-- Approach A: Exact Calculation from fact_table_usage (Detail Grain)
select
    full_table_name,
    count(distinct user_key) as distinct_users_exact,
    count(query_id) as total_queries_count,
    round(sum(allocated_cpu_time_ms) / 1000.0, 2) as total_allocated_cpu_seconds,
    round(sum(allocated_bytes_scanned) / (1024.0 * 1024.0 * 1024.0), 3) as total_allocated_gb_scanned
from {{ ref('fact_table_usage') }}
where full_table_name = 'iceberg.sales.orders'
  and execution_date between date '2026-11-01' and date '2026-11-07'
group by
    full_table_name;

-- Approach B: Sub-second Rollup from Mart Layer using Trino Native HyperLogLog (HLL) Sketch
-- Solves the non-additive distinct count problem across multi-day / multi-month periods without raw scans.
select
    full_table_name,
    sum(total_queries_count) as total_queries_count,
    -- Merge daily HLL sketches into a single multi-day sketch and extract cardinality
    cardinality(merge(cast(hll_users_sketch as HyperLogLog))) as distinct_users_hll_approx,
    round(sum(allocated_cpu_seconds), 2) as total_allocated_cpu_seconds,
    round(sum(allocated_gb_scanned), 3) as total_allocated_gb_scanned
from {{ ref('mart_daily_table_usage') }}
where full_table_name = 'iceberg.sales.orders'
  and usage_date between date '2026-11-01' and date '2026-11-07'
group by
    full_table_name;
