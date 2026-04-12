"""Quick-check: scan Bronze Parquet files with DuckDB."""
import duckdb

BRONZE_PATH = "data/bronze"
TABLES = ["vehicle_positions", "trip_updates", "service_alerts"]

con = duckdb.connect()

for table in TABLES:
    path = f"{BRONZE_PATH}/{table}/**/*.parquet"
    try:
        result = con.execute(f"""
            SELECT COUNT(*) as rows FROM read_parquet('{path}')
        """).fetchone()
        print(f"{table}: {result[0]} rows")

        # show a few recent records
        df = con.execute(f"""
            SELECT * FROM read_parquet('{path}')
            ORDER BY event_ts DESC
            LIMIT 5
        """).df()
        print(df.to_string(index=False))
        print()
    except Exception as e:
        print(f"{table}: no data yet ({e})\n")
