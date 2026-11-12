import json
import random
import uuid
from datetime import datetime, timedelta, timezone

USERS = [
    ("jkowalski", "Marketing Analytics"),
    ("anowak", "Risk Modeling"),
    ("pwisniewski", "Data Platform"),
    ("mzielinska", "Finance Reporting"),
    ("akozlowski", "Data Science"),
    ("kszymanski", "Marketing Analytics"),
    ("mlasocka", "Product Analytics"),
    ("kwojcik", "Risk Modeling"),
    ("airflow", "Unassigned / Service Account"),
    ("superset_reader", "Unassigned / Service Account"),
    ("unknown_intern", "Unassigned / Service Account"),
]

TABLE_POOL = [
    "iceberg.sales.orders",
    "iceberg.sales.order_items",
    "iceberg.crm.customers",
    "iceberg.finance.invoices",
    "iceberg.marketing.campaign_clicks",
    "iceberg.analytics.daily_active_users",
    "iceberg.risk.credit_scores",
    "iceberg.risk.transaction_monitoring",
]

def generate_mock_events(count: int = 2000, days_back: int = 14):
    events = []
    base_time = datetime.now(timezone.utc)

    for i in range(count):
        user, _ = random.choice(USERS)
        # Random query execution timestamp within the last N days
        offset_seconds = random.randint(0, days_back * 86400)
        start_time = base_time - timedelta(seconds=offset_seconds)
        
        # Duration: from 100ms to 120s
        duration_ms = int(random.expovariate(1.0 / 4000) + 150)
        
        # Determine number of tables touched: 0 (DDL/DML), 1 (simple scan), or 2-4 (joins)
        table_draw = random.random()
        if table_draw < 0.05:
            queried_tables = []
        elif table_draw < 0.65:
            queried_tables = [random.choice(TABLE_POOL)]
        else:
            num_tables = random.randint(2, 4)
            queried_tables = random.sample(TABLE_POOL, num_tables)

        # Trino parallelism: cpu_time is usually multi-core (1x to 16x of wall_time)
        parallelism = random.uniform(1.2, 14.5)
        cpu_time_ms = int(duration_ms * parallelism)
        
        # Bytes scanned: correlated with duration and table count
        bytes_scanned = int(len(queried_tables) * random.randint(10, 500) * 1024 * 1024)
        peak_memory_bytes = int(random.uniform(50, 4096) * 1024 * 1024)

        # Query state
        query_state = "FINISHED" if random.random() < 0.96 else random.choice(["FAILED", "USER_CANCELED"])

        query_id = f"{start_time.strftime('%Y%m%d_%H%M%S')}_{i:05d}_{uuid.uuid4().hex[:5]}"
        completion_time = start_time + timedelta(milliseconds=duration_ms)

        events.append({
            "query_id": query_id,
            "user_name": user,
            "query_state": query_state,
            "cpu_time_ms": cpu_time_ms,
            "wall_time_ms": duration_ms,
            "bytes_scanned": bytes_scanned,
            "peak_memory_bytes": peak_memory_bytes,
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S.%f") + " UTC",
            "completion_time": completion_time.strftime("%Y-%m-%d %H:%M:%S.%f") + " UTC",
            "tables": queried_tables
        })

    return events

if __name__ == "__main__":
    generated = generate_mock_events(1000)
    print(f"Successfully generated {len(generated)} realistic telemetry records.")
    # Preview sample record
    print("Sample generated record:")
    print(json.dumps(generated[0], indent=2))
