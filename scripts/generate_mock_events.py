import argparse
import json
import os
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone

try:
    import trino
except ImportError:
    trino = None

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

def generate_mock_events(count: int = 1500, days_back: int = 14):
    events = []
    base_time = datetime.now(timezone.utc)

    for i in range(count):
        user, _ = random.choice(USERS)
        # Random query execution timestamp within the last N days
        offset_seconds = random.randint(0, days_back * 86400)
        start_time = base_time - timedelta(seconds=offset_seconds)

        # Duration: from 100ms to 120s (exponentially distributed)
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

        # Trino parallelism: cpu_time is multi-threaded across workers (1.2x to 14.5x duration)
        parallelism = random.uniform(1.2, 14.5)
        cpu_time_ms = int(duration_ms * parallelism)

        # Bytes scanned: correlated with duration and table count
        bytes_scanned = int(len(queried_tables) * random.randint(10, 500) * 1024 * 1024)
        peak_memory_bytes = int(random.uniform(50, 4096) * 1024 * 1024)

        # Query state distribution: 94% FINISHED, 4% FAILED, 2% USER_CANCELED
        state_draw = random.random()
        if state_draw < 0.94:
            query_state = "FINISHED"
        elif state_draw < 0.98:
            query_state = "FAILED"
        else:
            query_state = "USER_CANCELED"

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

def create_and_load_trino(events, host="localhost", port=8080, catalog="iceberg", schema="events"):
    if trino is None:
        raise RuntimeError("The 'trino' Python package is not installed. Run: pip install trino")

    print(f"Connecting to Trino coordinator at {host}:{port} (catalog={catalog})...")
    conn = trino.dbapi.connect(
        host=host,
        port=port,
        user="admin",
        catalog=catalog,
        schema="default"
    )
    cur = conn.cursor()

    # 1. Ensure target schema exists
    print(f"Ensuring schema {catalog}.{schema} exists...")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # 2. Ensure Iceberg raw telemetry table exists
    target_table = f"{catalog}.{schema}.raw_trino_events"
    print(f"Ensuring target Iceberg table {target_table} exists...")
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        query_id VARCHAR,
        user_name VARCHAR,
        query_state VARCHAR,
        cpu_time_ms BIGINT,
        wall_time_ms BIGINT,
        bytes_scanned BIGINT,
        peak_memory_bytes BIGINT,
        start_time TIMESTAMP(6) WITH TIME ZONE,
        completion_time TIMESTAMP(6) WITH TIME ZONE,
        tables ARRAY(VARCHAR)
    ) WITH (
        format = 'PARQUET',
        partitioning = ARRAY['day(start_time)']
    )
    """)

    # 3. Batch insert records
    batch_size = 100
    print(f"Loading {len(events)} events into {target_table} in batches of {batch_size}...")
    values_list = []
    inserted_count = 0

    for e in events:
        tables_literal = "ARRAY[" + ", ".join([f"'{t}'" for t in e['tables']]) + "]" if e['tables'] else "ARRAY[]"
        values_list.append(f"""(
            '{e['query_id']}',
            '{e['user_name']}',
            '{e['query_state']}',
            {e['cpu_time_ms']},
            {e['wall_time_ms']},
            {e['bytes_scanned']},
            {e['peak_memory_bytes']},
            TIMESTAMP '{e['start_time']}',
            TIMESTAMP '{e['completion_time']}',
            {tables_literal}
        )""")

        if len(values_list) >= batch_size:
            cur.execute(f"INSERT INTO {target_table} VALUES {', '.join(values_list)}")
            inserted_count += len(values_list)
            print(f"  Inserted {inserted_count}/{len(events)} records...")
            values_list = []

    if values_list:
        cur.execute(f"INSERT INTO {target_table} VALUES {', '.join(values_list)}")
        inserted_count += len(values_list)
        print(f"  Inserted {inserted_count}/{len(events)} records...")

    print(f"SUCCESS: Successfully loaded {inserted_count} events into {target_table}!")

def main():
    parser = argparse.ArgumentParser(description="Generate and load realistic Trino QueryCompletedEvent telemetry.")
    parser.add_argument("--count", type=int, default=1500, help="Number of telemetry events to generate (default: 1500)")
    parser.add_argument("--days", type=int, default=14, help="Historical time span in days (default: 14)")
    parser.add_argument("--host", type=str, default="localhost", help="Trino coordinator hostname (default: localhost)")
    parser.add_argument("--port", type=int, default=8080, help="Trino coordinator port (default: 8080)")
    parser.add_argument("--catalog", type=str, default="iceberg", help="Target Iceberg catalog (default: iceberg)")
    parser.add_argument("--schema", type=str, default="events", help="Target schema (default: events)")
    parser.add_argument("--dry-run", action="store_true", help="Generate events without loading to Trino")
    parser.add_argument("--output-json", type=str, default=None, help="Optional path to dump generated JSON events")
    args = parser.parse_args()

    print(f"Generating {args.count} realistic Trino telemetry events spanning {args.days} days...")
    events = generate_mock_events(count=args.count, days_back=args.days)
    print(f"Generated {len(events)} records.")

    if args.output_json:
        os.makedirs(os.path.dirname(os.path.abspath(args.output_json)), exist_ok=True)
        with open(args.output_json, "w", encoding="utf-8") as f:
            json.dump(events, f, indent=2)
        print(f"Dumped generated telemetry to {args.output_json}")

    if args.dry_run:
        print("Dry run requested. Skipping database load.")
        print("Sample generated record:")
        print(json.dumps(events[0], indent=2))
        return

    try:
        create_and_load_trino(
            events=events,
            host=args.host,
            port=args.port,
            catalog=args.catalog,
            schema=args.schema
        )
    except Exception as exc:
        print(f"\n[NOTICE] Unable to load directly to Trino ({exc}).")
        print("To load into a running cluster, ensure Docker Compose is running: docker compose up -d")
        print("Sample preview of generated telemetry:")
        print(json.dumps(events[0], indent=2))

if __name__ == "__main__":
    main()
