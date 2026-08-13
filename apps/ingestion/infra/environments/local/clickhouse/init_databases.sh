#!/bin/bash
set -e


# 0. Setup Authentication (Inherited from Compose Environment)
USER=${CLICKHOUSE_USER:-default}
PASS=${CLICKHOUSE_PASSWORD:-password}
HOST=${CH_HOST:-localhost}

ch_client() {
    clickhouse-client --host "$HOST" --user "$USER" --password "$PASS" --multiquery "$@"
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

# Add this setting to ensure the "Replace" logic has permission to swap tables
ch_client -q "SET allow_experimental_refreshable_materialized_view = 1"

# Get all refreshable views and run SYSTEM STOP on each
ch_client -q "SYSTEM STOP VIEWS" || true

# NEW: Force clear any stuck refreshes by killing active queries
echo "🧹 Killing any lingering refresh queries..."
ch_client -q "KILL QUERY WHERE query_kind = 'RefreshView' ASYNC" || true

# 2. Define the EXPLICIT order of SQL execution
# Add your table definitions here in the order they should be created
SQL_FILES=(
    "/sql/definitions/tables/test.orders.sql"
    "/sql/definitions/tables/test.sim_data.sql"
    "/sql/definitions/tables/meta.execution_log.sql"
    "/sql/definitions/tables/meta.job_schedules.sql"
    "/sql/definitions/tables/meta.execution_history.sql"
    "/sql/definitions/views/meta.current_schedules.sql"
    "/sql/definitions/views/meta.append_log_trigger.sql"
    "/sql/definitions/views/meta.current_execution.sql"
    "/sql/definitions/views/meta.execution_history.sql"
    "/sql/definitions/views/meta.error_log.sql"
    "/sql/definitions/views/meta.daemon_task_poll.sql"
    "/sql/adhoc/seed_metadata.sql"
)


# Execute SQL files in defined order
for f in "${SQL_FILES[@]}"; do
    if [ -f "$f" ]; then
        echo "📑 Executing: $f"
        ch_client < "$f"
    else
        echo "❌ Error: SQL file not found at $f. Verify volume mount!"
    fi
done

# 3. Resume Refreshes
echo "▶️ Resuming background refreshes..."
# Get all refreshable views and run SYSTEM START on each
VIEWS=$(ch_client -q "SELECT concat(database, '.', view) FROM system.view_refreshes WHERE database IN ('META', 'TEST')")

for v in $VIEWS; do
    echo "  Starting: $v"
    ch_client -q "SYSTEM START VIEW $v"
done

echo "📊 Current View Status:"
ch_client -q "SELECT view, status, exception FROM system.view_refreshes" --format PrettyCompact

echo "✅ ClickHouse initialization complete."
