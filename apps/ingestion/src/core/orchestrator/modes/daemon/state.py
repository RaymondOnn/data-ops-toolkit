"""Daemon-only logic for observing database state and handling background events."""

import msgspec
import polars as pl
from loguru import logger

from src.core.contexts.execution import ExecutionContext
from src.core.contexts.task import TaskContext
from src.core.models.task import ExecutionStatus, TaskManifest
from src.core.orchestrator.common.state import StateHub
from src.core.orchestrator.enums import (
    TaskRecord,
)
from src.utils.constants import (
    MANIFEST_FILENAME,
)

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

        LOG.trace("DaemonStateObserver initialized")

    def refresh(self, lookahead_minutes: int = 60) -> dict[str, TaskRecord]:
        """
        Poll the database for active/pending jobs and update the registry.

        Args:
            lookahead_minutes: Minutes into the future to look for scheduled runs.

        Returns:
            dict[str, TaskRecord]: Mapping of Run IDs to TaskRecord objects.
        """
        # active_statuses = [f"'{s.value}'" for s in ExecutionStatus.active_statuses()]
        # status_filter = ", ".join(active_statuses)

        # TODO: Compute query via SQLCompiler
        sql = "SELECT * FROM META.DAEMON_TASK_POLL"

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

        # Query directly using fetch_df() - column names are automatically included
        for df in self.hub.sink.db.fetch_df(sql):
            processed_df = df

            # 1. Cast Binary / BinaryView columns to Utf8 to prevent write_json panic
            binary_cols = [
                c
                for c, dtype in processed_df.schema.items()
                if dtype == pl.Binary or "Binary" in str(dtype)
            ]
            if binary_cols:
                processed_df = processed_df.with_columns(
                    pl.col(col).cast(pl.Utf8, strict=False) for col in binary_cols
                )

            # 2. Strip trailing null bytes from Utf8 columns
            str_cols = [
                c for c, dtype in processed_df.schema.items() if dtype == pl.Utf8
            ]
            if str_cols:
                processed_df = processed_df.with_columns(
                    pl.col(col).str.strip_chars("\x00") for col in str_cols
                )

            # 3. Format datetimes directly inside Polars
            datetime_cols = [
                c for c, dtype in processed_df.schema.items() if dtype.is_temporal()
            ]
            if datetime_cols:
                processed_df = processed_df.with_columns(
                    pl.col(col).dt.strftime("%Y-%m-%d %H:%M:%S")
                    for col in datetime_cols
                )

            # 4. Safely serialize to JSON and parse with msgspec
            json_bytes = processed_df.write_json().encode("utf-8")
            try:
                parsed_records = msgspec.json.decode(json_bytes, type=list[TaskRecord])
                for rec in parsed_records:
                    records[rec.RUN_ID] = rec
            except msgspec.ValidationError:
                # Fallback to row-by-row conversion if a bad record breaks batch decoding
                for raw_dict in processed_df.to_dicts():
                    try:
                        record = msgspec.convert(raw_dict, TaskRecord)
                        records[record.RUN_ID] = record
                    except msgspec.ValidationError:
                        LOG.warning(
                            "Failed to convert record", run_id=raw_dict.get("RUN_ID")
                        )

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
            run_id=run.RUN_ID,
        )
        manifest = self._try_load_manifest(run.RUN_ID)

        if not manifest:
            manifest = TaskManifest(
                job_id=run.JOB_ID,
                run_id=run.RUN_ID,
                dataset_id=run.DATASET_ID,
                status=ExecutionStatus.PENDING,
                current_step_id="N/A",
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
