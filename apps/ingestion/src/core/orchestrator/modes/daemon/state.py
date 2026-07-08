"""Daemon-only logic for observing database state and handling background events."""

from datetime import datetime

import msgspec
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.contexts.task import TaskContext
from apps.ingestion.src.core.models.task import ExecutionStatus, TaskManifest
from apps.ingestion.src.core.orchestrator.common.state import StateHub
from apps.ingestion.src.core.orchestrator.enums import (
    TaskRecord,
    to_ch_datetime,
)
from apps.ingestion.src.utils.constants import (
    MANIFEST_FILENAME,
)
from loguru import logger

LOG = logger
SOURCE_TABLE = "META.CURRENT_EXECUTION"


class DaemonState:
    """
    Daemon-only logic for observing database state and handling background events.

    Composes StateHub to update the active registry with records from the database.
    Used exclusively in daemon mode for polling and expiry handling.
    """

    def __init__(self, hub: StateHub, exec_ctx: ExecutionContext):
        """
        Initialize the daemon state observer.

        Args:
            hub: The base StateHub instance for state operations.
            exec_ctx: The global execution context.
        """
        self.hub = hub
        self.exec_ctx = exec_ctx
        self._column_cache: list[str] | None = None

        LOG.debug("DaemonStateObserver initialized")

    def refresh(self, lookahead_minutes: int = 60) -> dict[str, TaskRecord]:
        """
        Poll the database for active/pending jobs and update the registry.

        Args:
            lookahead_minutes: Minutes into the future to look for scheduled runs.

        Returns:
            dict[str, TaskRecord]: Mapping of Run IDs to TaskRecord objects.
        """
        active_statuses = [f"'{s.value}'" for s in ExecutionStatus.active_statuses()]
        status_filter = ", ".join(active_statuses)

        sql = f"""
            WITH dates AS (
                SELECT
                    8 AS OFFSET_HOURS,
                    now64(3) + INTERVAL OFFSET_HOURS HOUR AS NOW_TS_LC
            )
            SELECT * FROM {SOURCE_TABLE}
            WHERE JOB_STATUS IN ({status_filter})
            AND RUN_ID IS NOT NULL
            AND SCHEDULED_TIMESTAMP_LC <= (SELECT NOW_TS_LC FROM dates)
            + INTERVAL {lookahead_minutes} MINUTE
        """

        try:
            records = self._fetch_records(sql)

            with self.hub.store._lock:
                self.hub.store.records = records

            LOG.info(f"Polled {len(records)} active runs from database")
            return records

        except Exception:
            LOG.exception("Failed to poll database state")
            return self.hub.store.records

    def _fetch_records(self, sql: str) -> dict[str, TaskRecord]:
        """Execute SQL and convert results to TaskRecord dictionary."""
        records = {}

        # Get or cache column schema
        if not self._column_cache:
            self._column_cache = [
                row[0]
                for row in self.hub.sink.db.fetch(f"DESCRIBE TABLE {SOURCE_TABLE}")
            ]

        raw_records = self.hub.sink.db.fetch(sql)

        for raw_row in raw_records:
            raw_dict = dict(zip(self._column_cache, raw_row, strict=False))

            # Clean up values
            for key, value in raw_dict.items():
                if isinstance(value, bytes):
                    raw_dict[key] = value.decode("utf-8").rstrip("\x00")
                elif isinstance(value, datetime):
                    raw_dict[key] = to_ch_datetime(value)

            try:
                record = msgspec.convert(raw_dict, TaskRecord)
                records[record.RUN_ID] = record
            except msgspec.ValidationError:
                LOG.warning("Failed to convert record", run_id=raw_dict.get("RUN_ID"))
                continue

        return records

    def log_expiry(
        self,
        run: TaskRecord,
        context: TaskContext | None = None,
        reason: str = "TTL Expired",
    ) -> None:
        """
        Emit an expiry event for a run to the state stream.

        Args:
            run: The expired job record.
            context: Optional task context.
            reason: Reason for expiry.
        """
        ctx = context or TaskContext(
            job_id=run.JOB_ID,
            dataset_id=run.DATASET_ID,
            partition_date=str(run.PARTITION_DATE),
        )
        manifest = self._try_load_manifest(run.RUN_ID)

        if not manifest:
            manifest = TaskManifest(
                job_id=run.JOB_ID,
                run_id=run.RUN_ID,
                dataset_id=run.DATASET_ID,
                status=ExecutionStatus.PENDING,
                current_stage="N/A",
                bitmask=0,
            )

        # Pass metadata to override status and add remarks
        self.hub.source.parse_update(
            manifest=manifest,
            context=ctx,
            deep_sync=False,
            metadata={
                "status": ExecutionStatus.EXPIRED,
                "remarks": reason,
            },
        )

    def _try_load_manifest(self, run_id: str) -> TaskManifest | None:
        """Load manifest from disk if it exists."""
        path = self.hub.find_task_path(run_id)

        if not path:
            return None

        manifest_file = path / MANIFEST_FILENAME
        if not manifest_file.exists():
            return None

        try:
            with manifest_file.open("rb") as f:
                return msgspec.json.decode(f.read(), type=TaskManifest)
        except Exception as e:
            LOG.debug(f"Failed to load manifest for {run_id}: {e}")
            return None
