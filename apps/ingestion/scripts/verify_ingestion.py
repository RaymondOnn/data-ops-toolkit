import sys
from pathlib import Path

import polars as pl
from src.core.strategies.load.load import Loader, WriteContext
from src.services.database import ClickHouseService


# Mocking Secret for independent testing
class MockSecret:
    def __init__(self, value):
        self._value = value

    def resolve(self, sanitize=False):
        return self._value


# Add apps/ingestion to sys.path to allow imports from src
sys.path.append(str(Path(__file__).parent.parent))


def main():
    # 1. Configuration
    csv_path = Path(__file__).parent.parent / "samples" / "sample_orders.csv"
    parquet_dir = Path(__file__).parent.parent / "tmp" / "silver_orders"
    target_table = "orders"

    # Ensure parquet_dir exists
    parquet_dir.mkdir(parents=True, exist_ok=True)

    print("--- Step 1: Converting CSV to Parquet (Simulating Silver Stage) ---")
    df = pl.read_csv(csv_path)
    # Save as parquet since ClickHouseService currently expects parquet files in a directory
    df.write_parquet(parquet_dir / "part-0.parquet")
    print(f"Converted {len(df)} rows to Parquet at {parquet_dir}")

    print("\n--- Step 2: Initializing ClickHouseService ---")
    # Using default ports from docker-compose
    config = {
        "host": "localhost",
        "port": 8123,
        "user": "default",
        "password": MockSecret("password"),
    }

    service = ClickHouseService(name="clickhouse_test", **config)

    # Pre-step: Create the target table in ClickHouse
    print(f"Checking if table '{target_table}' exists...")
    create_table_sql = f""" 
    CREATE TABLE IF NOT EXISTS {target_table} (
        order_id Int64,
        customer_id Int64,
        order_date Date,
        amount Float64,
        status String,
        updated_at DateTime
    ) ENGINE = MergeTree()
    ORDER BY order_id
    PARTITION BY toYYYYMMDD(updated_at)
    """
    service.client.sql(create_table_sql)
    print(f"Table '{target_table}' ready.")

    print("\n--- Step 3: Executing Loader.load (Staging) ---")
    loader = Loader()
    # ClickHouseService.stage_data expects source_dir
    # We mount ./tmp to /var/lib/clickhouse/user_files/tmp
    # So the path ClickHouse sees is just /var/lib/clickhouse/user_files/tmp/silver_orders
    # clickhouse_connect and 'file()' function usually look relative to user_files
    staging_result = loader.load(service, "tmp/silver_orders", target_table)
    print(f"Data staged in temporary table: {staging_result.staging_table}")

    # Debug: Check staging table
    staging_rows = service.client.sql(
        f"SELECT count() FROM {staging_result.staging_table}"
    )
    print(
        f"Rows in staging table '{staging_result.staging_table}': {staging_rows[0][0]}"
    )

    staging_parts = service.client.sql(
        f"SELECT partition FROM system.parts WHERE table = '{staging_result.staging_table}' AND database = 'default'"
    )
    print(f"Partitions in staging table: {[p[0] for p in staging_parts]}")

    print("\n--- Step 4: Executing Loader.promote (Atomic Swap) ---")
    write_ctx = WriteContext(
        target_destination=target_table,
        partition_col="updated_at",
        partition_value="20260310",  # This should match a partition for testing
    )

    # Note: ClickHouseService.promote_data uses 'REPLACE PARTITION' which requires specific partition ID
    # In our sample data, we have multiple days. We'll just test the mechanism.
    try:
        loader.promote(service, staging_result, write_ctx)
        print("Promotion successful.")
    except Exception as e:
        print(f"Promotion failed (as expected if partition value didn't match): {e}")

    print("\n--- Step 5: Verification ---")
    count_result = service.client.sql(f"SELECT count() FROM {target_table}")
    print("Total rows in '{target_table}': {count_result[0][0]}")

    print("\n--- DONE ---")


if __name__ == "__main__":
    main()
