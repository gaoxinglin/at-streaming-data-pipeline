"""
Sync Bronze Parquet into a persistent DuckDB file for dev use.

Run periodically (e.g. daily via cron) or on-demand:
    python scripts/sync_to_duckdb.py

Creates/replaces tables in data/at_bronze.duckdb so dbt-duckdb
or ad-hoc queries can work without scanning loose Parquet files.
"""
import duckdb

BRONZE_PATH = "data/bronze"
DB_PATH = "data/at_bronze.duckdb"
TABLES = ["vehicle_positions", "trip_updates", "service_alerts"]

con = duckdb.connect(DB_PATH)

for table in TABLES:
    path = f"{BRONZE_PATH}/{table}/**/*.parquet"
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT * FROM read_parquet('{path}')
        """)
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"{table}: {count} rows synced")
    except Exception as e:
        print(f"{table}: skipped ({e})")

con.close()
print(f"\nDone — {DB_PATH}")
