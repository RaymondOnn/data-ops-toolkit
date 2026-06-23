import json
import logging
from typing import Any

import msgspec
from libs.database.clients.postgres import PostgresClient

from .base import PriorityQueue, TaskMessage

LOG = logging.getLogger(__name__)


class SQLQueue(PriorityQueue):
    """Production queue using PostgreSQL with SKIP LOCKED via PostgresClient."""

    def __init__(
        self,
        postgres_client: PostgresClient,
        table_name: str = "task_queue",
        auto_migrate: bool = True,
    ):
        """Initialize SQL queue with PostgresClient.

        Args:
            postgres_client: Your existing PostgresClient instance
            table_name: Queue table name
            auto_migrate: Auto-create table if missing
        """
        self.client = postgres_client
        self.table_name = table_name
        self._initialized = False

        if auto_migrate:
            self._ensure_table()

        LOG.info(
            f"SQLQueue initialized | Table: {table_name} | Client: {postgres_client.type}"
        )

    def _ensure_table(self) -> None:
        """Create queue table if it doesn't exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id BIGSERIAL PRIMARY KEY,
            priority INTEGER NOT NULL,
            group_id TEXT,
            data JSONB NOT NULL,
            metadata JSONB DEFAULT '{{}}'::jsonb,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            processing_start TIMESTAMP WITH TIME ZONE,
            processing_timeout TIMESTAMP WITH TIME ZONE,
            attempts INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_priority_created
        ON {self.table_name} (priority ASC, created_at ASC);

        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_processing
        ON {self.table_name} (processing_start, processing_timeout);

        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_group
        ON {self.table_name} (group_id);
        """

        try:
            self.client.sql(create_table_sql)
            self._initialized = True
            LOG.debug(f"SQLQueue table {self.table_name} ensured")
        except Exception as e:
            LOG.error(f"Failed to create queue table: {e}")
            raise

    def push(
        self,
        data: Any,
        priority: int,
        group: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Insert task with priority."""
        # Convert data to JSON-serializable format
        if hasattr(data, "to_dict"):
            data = data.to_dict()
        elif hasattr(data, "__dataclass_fields__"):
            data = msgspec.to_builtins(data)

        # Ensure we can serialize to JSON
        try:
            data_json = json.dumps(data)
            metadata_json = json.dumps(metadata or {})
        except TypeError as e:
            LOG.error(f"Cannot serialize data to JSON: {e}")
            raise ValueError(f"Data must be JSON-serializable: {e}") from e

        insert_sql = f"""
        INSERT INTO {self.table_name}
        (priority, group_id, data, metadata)
        VALUES ({priority}, %s, %s::jsonb, %s::jsonb)
        """.format(group, data_json, metadata_json)

        self.client.sql(insert_sql)
        LOG.debug(f"Pushed task with priority {priority}, group={group}")

    def pop(self, visibility_timeout: int = 300) -> TaskMessage | None:
        """Atomic pop using SKIP LOCKED via PostgresClient."""
        # Use PostgreSQL's SKIP LOCKED for atomic dequeue
        pop_sql = f"""
        WITH locked_task AS (
            SELECT id, data, metadata, attempts
            FROM {self.table_name}
            WHERE (processing_start IS NULL OR processing_timeout < NOW())
            ORDER BY priority ASC, created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE {self.table_name} AS t
        SET
            processing_start = NOW(),
            processing_timeout = NOW() + INTERVAL '{visibility_timeout} seconds',
            attempts = t.attempts + 1
        FROM locked_task
        WHERE t.id = locked_task.id
        RETURNING t.id, t.data, t.metadata, t.attempts
        """

        try:
            result = self.client.sql(pop_sql)

            if not result or len(result) == 0:
                return None

            # Parse result
            row = result[0]  # tuple: (id, data, metadata, attempts)
            task_id = str(row[0])
            data = json.loads(row[1])
            metadata = json.loads(row[2]) if row[2] else {}

            return TaskMessage(id_=task_id, data=data, metadata=metadata)

        except Exception as e:
            LOG.error(f"Failed to pop task: {e}")
            return None

    def ack(self, msg_id: str) -> None:
        """Acknowledge and delete completed task."""
        delete_sql = f"DELETE FROM {self.table_name} WHERE id = {msg_id}"
        self.client.sql(delete_sql)
        LOG.debug(f"Acknowledged task {msg_id}")

    def size(self) -> int:
        """Return pending task count."""
        count_sql = f"""
        SELECT COUNT(*) FROM {self.table_name}
        WHERE processing_start IS NULL OR processing_timeout < NOW()
        """

        try:
            result = self.client.sql(count_sql)
            return int(result[0][0]) if result else 0
        except Exception as e:
            LOG.error(f"Failed to get queue size: {e}")
            return 0

    def cleanup(self) -> None:
        """Clean up old completed tasks."""
        # Remove tasks older than 7 days
        cleanup_sql = f"""
        DELETE FROM {self.table_name}
        WHERE processing_start IS NOT NULL
        AND processing_start < NOW() - INTERVAL '7 days'
        """
        self.client.sql(cleanup_sql)
        LOG.debug("SQLQueue cleanup completed")

    def shutdown(self) -> None:
        """Shutdown - connection managed by PostgresClient."""
        pass

    def get_stats(self) -> dict[str, Any]:
        """Get queue statistics."""
        stats_sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(CASE WHEN processing_start IS NULL THEN 1 END) AS pending,
            COUNT(CASE WHEN processing_start IS NOT NULL AND processing_timeout > NOW() THEN 1 END) AS processing,
            COUNT(CASE WHEN attempts > 3 THEN 1 END) AS stalled,
            AVG(attempts) AS avg_attempts
        FROM {self.table_name}
        """

        try:
            result = self.client.sql(stats_sql)
            if result:
                row = result[0]
                return {
                    "total": int(row[0]),
                    "pending": int(row[1]),
                    "processing": int(row[2]),
                    "stalled": int(row[3]),
                    "avg_attempts": float(row[4]) if row[4] else 0,
                }
        except Exception as e:
            LOG.error(f"Failed to get stats: {e}")

        return {}
