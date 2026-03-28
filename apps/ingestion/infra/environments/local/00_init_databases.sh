#!/bin/bash
set -e

# 1. Define the databases needed for the Ingestion Pipeline
# These represent the different layers of your data lake/warehouse
DATABASES=("meta" "test")

echo "🚀 Starting ClickHouse Object Coordination..."


# 2. Define the EXPLICIT order of SQL execution
# Add your table definitions here in the order they should be created
SQL_FILES=(
    "/docker-entrypoint-initdb.d/test.orders.sql"
    # "/docker-entrypoint-initdb.d/orchestrator_setup.sql"
    # "/docker-entrypoint-initdb.d/raw_tables.sql"
    # "/docker-entrypoint-initdb.d/silver_tables.sql"
)

echo "🚀 Starting ClickHouse Object Coordination..."

# Create Databases
for db in "${DATABASES[@]}"; do
    echo "Initializing database: $db"
    clickhouse-client -q "CREATE DATABASE IF NOT EXISTS $db"
done

# Execute SQL files in defined order
for f in "${SQL_FILES[@]}"; do
    if [ -f "$f" ]; then
        echo "Processing SQL file: $f"
        clickhouse-client -n < "$f"
    else
        echo "⚠️ Warning: SQL file not found at $f. Skipping..."
    fi
done

echo "✅ ClickHouse initialization complete."