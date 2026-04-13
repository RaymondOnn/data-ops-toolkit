#!/bin/bash
set -e


# 0. Setup Authentication (Inherited from Compose Environment)
USER=${CLICKHOUSE_USER:-default}
PASS=${CLICKHOUSE_PASSWORD:-password}
HOST=${CH_HOST:-localhost}

ch_client() {
    clickhouse-client --host "$HOST" --user "$USER" --password "$PASS" "$@"
}

# 0.5 Wait for ClickHouse to be responsive
echo "⏳ Waiting for ClickHouse at $HOST..."
until ch_client -q "SELECT 1" > /dev/null 2>&1; do
    echo "   ...still waiting"
    sleep 2
done

if [ "$HOST" != "localhost" ]; then
    echo "📡 Connected to remote ClickHouse at $HOST"
else
    echo "🏠 Connected to local ClickHouse"
fi

# 1. Define the databases needed for the Ingestion Pipeline
# These represent the different layers of your data lake/warehouse
DATABASES=("META" "TEST")

echo "🚀 Starting ClickHouse Object Coordination..."

# Create Databases
for db in "${DATABASES[@]}"; do
    echo "Initializing database: $db"
    ch_client -q "CREATE DATABASE IF NOT EXISTS $db"
done

# 1.5 Global Stop Refreshes
# This prevents background tasks from conflicting with object creation/replacement.
echo "🛑 Pausing background refreshes to prevent race conditions..."
ch_client -q "SYSTEM STOP VIEW REFRESHES" || true

# 2. Define the EXPLICIT order of SQL execution
# Add your table definitions here in the order they should be created
SQL_FILES=(
    "/sql/definitions/tables/test.orders.sql"
    "/sql/definitions/tables/meta.execution_log.sql"
    "/sql/definitions/tables/meta.job_schedules.sql"
    "/sql/definitions/tables/meta.execution_history.sql"
    "/sql/definitions/views/meta.execution_history.sql"
    "/sql/definitions/views/meta.error_log.sql"
    "/sql/definitions/views/meta.current_execution.sql"
    "/sql/definitions/views/meta.current_schedules.sql"
    "/sql/definitions/views/meta.work_queue_trigger.sql"
    "/sql/adhoc/seed_metadata.sql"
    "/sql/definitions/tables/meta.execution_log_clone.sql"
)


# Execute SQL files in defined order
for f in "${SQL_FILES[@]}"; do
    if [ -f "$f" ]; then
        echo "Processing SQL file: $f"
        ch_client -n < "$f"
    else
        echo "⚠️ Warning: SQL file not found at $f. Skipping..."
    fi
done

# 3. Resume Refreshes
echo "▶️ Resuming background refreshes..."
ch_client -q "SYSTEM START VIEWS" || true

echo "✅ ClickHouse initialization complete."