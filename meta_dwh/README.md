# Trino Meta DWH – Lakehouse Observability & FinOps Platform

A production-grade metadata warehouse (Meta DWH) built with **dbt** (`dbt-trino`) and **Apache Iceberg** on **Amazon S3** to monitor Trino cluster utilization, table access patterns, and team resource consumption.

---

## 1. Executive Summary & Architecture

The platform processes telemetry emitted by Trino's `EventListener` (`QueryCompletedEvent`), cleans and normalizes it, and loads it into a **Kimball Star Schema** backed by Apache Iceberg tables.

```
                    +------------------------------------+
                    |  raw_trino_events (Iceberg on S3)  |
                    +------------------------------------+
                                      |
                                      v
                    +------------------------------------+
                    |        stg_raw_trino_events        |
                    |      (Grain: 1 row = 1 query)      |
                    +------------------------------------+
                                      |
                     +----------------+----------------+
                     |                                 |
                     v                                 v
    +--------------------------------+   +---------------------------+
    |   stg_raw_trino_event_tables   |   |   seed_user_teams (HR)    |
    | (Grain: 1 row = 1 query x tbl) |   +---------------------------+
    +--------------------------------+                 |
          |                     |                      v
          |                     |             +-----------------+
          |                     +------------>|    dim_user     |
          v                                   +-----------------+
    +-----------+                                      |
    | dim_table |                                      |
    +-----------+                                      |
          |                     +----------------------+
          |                     |                      |
          v                     v                      v
+------------------+  +-------------------+  +----------------------+
|    dim_table     |  | fact_table_usage  |  | fact_query_execution |
+------------------+  +-------------------+  +----------------------+
```

### Table Granularity & Schema Overview

| Layer | Model Name | Materialization | Grain | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Source** | `raw_trino_events` | Iceberg (S3) | 1 query completion | Raw append-only telemetry events from Trino Coordinator. |
| **Staging** | `stg_raw_trino_events` | View | 1 query execution | Filters `FINISHED`, types metrics, derives `execution_date` and `tables_queried_count`. |
| **Staging** | `stg_raw_trino_event_tables` | View | 1 query $\times$ 1 referenced table | Unnests `tables`, parses catalog/schema/table, and computes $1/N$ cost factor. |
| **Core Dim** | `dim_user` | Table (Iceberg) | 1 user | Combines HR seed with auto-discovered Trino principals; includes synthetic `__UNKNOWN_USER__` fallback. |
| **Core Dim** | `dim_table` | Incremental (Iceberg) | 1 unique table | Catalog of all referenced tables with lifetime query counts and `first_seen`/`last_seen` timestamps. |
| **Core Fact** | `fact_query_execution` | Incremental (Iceberg) | 1 query execution | Source of truth for cluster compute/memory; **zero double-counting** when aggregating. |
| **Core Fact** | `fact_table_usage` | Incremental (Iceberg) | 1 query $\times$ 1 referenced table | Table-level cost attribution using proportional ($1/N$) metrics. |

---

## 2. Key Engineering Design Decisions & Assumptions

### A. Cost Attribution Problem ($1/N$ Proportional Allocation)
* **The Challenge:** Trino's `QueryCompletedEvent` records `cpu_time_ms` and `bytes_scanned` at the **query level**. When a query joins 3 tables (`orders`, `customers`, `products`), Trino does not log table-level CPU breakdown.
* **The Solution:** We maintain two separate fact tables:
  1. `fact_query_execution`: Preserves the true, unadjusted cost of each query. Ideal for CFO / cluster-level billing and team-level rollups.
  2. `fact_table_usage`: Distributes query resources proportionally among referenced tables using:
     $$\text{cost\_allocation\_factor} = \frac{1.0}{\text{tables\_queried\_count}}$$
     $$\text{allocated\_cpu\_time\_ms} = \text{round}(\text{cpu\_time\_ms} \times \text{cost\_allocation\_factor})$$
  This guarantees mathematical consistency: summing allocated CPU for a single query across its tables equals the exact total query CPU.

### B. Apache Iceberg on Amazon S3
* **ACID Transactions & Snapshot Isolation:** Prevents dirty reads and corrupted writes during concurrent pipeline execution.
* **Incremental Merge Strategy:** Both fact tables and `dim_table` utilize `incremental_strategy = 'merge'`. On incremental runs, only new events (`start_time > (select max(start_time) from {{ this }})`) are processed, ensuring full idempotency.
* **Hidden Partitioning:** Tables are partitioned by `day(execution_date)`. Trino applies partition pruning directly without requiring manual partition column transformations.

### C. User-to-Team Attribution (HR Dimension)
* **The Assumption:** User mapping originates from an enterprise directory / HR system (`seed_user_teams.csv`).
* **Handling Missing Principals:** If an unknown user or service account (e.g., `airflow`, `superset_reader`) executes queries, `dim_user` captures them with `team_name = 'Unassigned / Service Account'` and `is_service_account = true`, while maintaining referential integrity via fallback key `__UNKNOWN_USER__`.

---

## 3. Business Analytics Queries

The project includes pre-compiled, production-ready analysis queries in `analyses/`:

### 1. How many distinct users queried table X during a given period?
📁 [analyses/distinct_users_per_table.sql](analyses/distinct_users_per_table.sql)

```sql
select
    full_table_name,
    count(distinct user_key) as distinct_users_count,
    min(start_time) as period_start,
    max(start_time) as period_end,
    count(query_id) as total_queries_count,
    round(sum(allocated_cpu_time_ms) / 1000.0, 2) as total_allocated_cpu_seconds,
    round(sum(allocated_bytes_scanned) / (1024.0 * 1024.0 * 1024.0), 3) as total_allocated_gb_scanned
from {{ ref('fact_table_usage') }}
where full_table_name = 'iceberg.sales.orders'
  and execution_date between date '2026-11-01' and date '2026-11-07'
group by
    full_table_name;
```

### 2. How many resources did team Y consume during a given period?
📁 [analyses/team_resource_consumption.sql](analyses/team_resource_consumption.sql)

```sql
select
    team_name,
    department,
    count(query_id) as total_queries_executed,
    round(sum(cpu_time_ms) / (1000.0 * 60.0 * 60.0), 2) as total_cpu_hours,
    round(sum(bytes_scanned) / (1024.0 * 1024.0 * 1024.0), 2) as total_gb_scanned,
    round(avg(duration_ms) / 1000.0, 2) as avg_query_duration_seconds,
    round(max(peak_memory_bytes) / (1024.0 * 1024.0 * 1024.0), 2) as max_peak_memory_gb
from {{ ref('fact_query_execution') }}
where team_name = 'Marketing Analytics'
  and execution_date between date '2026-11-01' and date '2026-11-30'
group by
    team_name,
    department;
```

### 3. Historical Usage Trends (Week-over-Week Cost & Query Evolution)
📁 [analyses/historical_usage_trends.sql](analyses/historical_usage_trends.sql)

Provides week-over-week growth metrics (`wow_cpu_growth_pct`, active user counts, and TB scanned) to detect scaling anomalies and optimize cluster capacity.

---

## 4. Running the Project

```bash
# 1. Parse and validate project syntax
dbt parse --profiles-dir .

# 2. Load HR reference seed data
dbt seed --profiles-dir .

# 3. Execute incremental models
dbt run --profiles-dir .

# 4. Run data quality and relational integrity tests
dbt test --profiles-dir .
```
