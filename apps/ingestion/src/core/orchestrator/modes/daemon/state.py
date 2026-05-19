from datetime import datetime

import msgspec
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.contexts.task import TaskContext
from apps.ingestion.src.core.models.task import ExecutionStatus, TaskManifest
from apps.ingestion.src.core.orchestrator.enums import (
    JobRecord,
    to_ch_datetime,
)
from apps.ingestion.src.utils.constants import (
    MANIFEST_FILENAME,
)
from loguru import logger

from ...common.state import StateStore

LOG = logger
SOURCE_TBL = "META.CURRENT_EXECUTION"


class DaemonStateStore:
    """
    Daemon-only logic for observing the DB state and handling background events.
    Composes StateStore to update the active registry.
    """

    def __init__(self, store: StateStore, exec_ctx: ExecutionContext):
        self.store = store
        self.exec_ctx = exec_ctx

    def get_latest_state(
        self, force_refresh: bool = False, lookahead_mins: int = 60
    ) -> dict[str, JobRecord]:
        """Polls the DB view for active/pending jobs and updates the store registry."""
        new_active_records = {}
        active_statuses = [f"'{s.value}'" for s in ExecutionStatus.active_statuses()]
        status_filter = ", ".join(active_statuses)

        # Debug: Check the ClickHouse "Now"
        ch_now = self.store.db.fetch("SELECT now64(3) + INTERVAL 8 HOUR AS NOW_TS_LC")[
            0
        ][0]

        sql = f"""
             WITH dates AS (
                SELECT
                    8 AS OFFSET_HOURS
                    , now64(3) + INTERVAL OFFSET_HOURS HOUR AS NOW_TS_LC
            ) 
            SELECT * FROM {SOURCE_TBL} 
            WHERE JOB_STATUS IN ({status_filter})
            AND RUN_ID IS NOT NULL
            AND SCHEDULED_TIMESTAMP_LC <= (SELECT NOW_TS_LC FROM dates) 
            + INTERVAL {lookahead_mins} MINUTE
        """

        try:
            if not self.store._src_column_cache:
                self.store._src_column_cache = [
                    row[0]
                    for row in self.store.db.fetch(f"DESCRIBE TABLE {SOURCE_TBL}")
                ]

            raw_records = self.store.db.fetch(sql)
            for r in raw_records:
                raw_dict = dict(zip(self.store._src_column_cache, r, strict=False))
                for k, v in raw_dict.items():
                    if isinstance(v, bytes):
                        raw_dict[k] = v.decode("utf-8").rstrip("\x00")
                    elif isinstance(v, datetime):
                        raw_dict[k] = to_ch_datetime(v)

                try:
                    record = msgspec.convert(raw_dict, JobRecord)
                    new_active_records[record.RUN_ID] = record
                except msgspec.ValidationError:
                    continue

            with self.store._registry_lock:
                self.store._active_records = new_active_records

            LOG.info(
                "Sync complete. Active Registry: {count} jobs",
                count=len(new_active_records),
            )
            return new_active_records

        except Exception:
            LOG.exception("Failed to monitor database state")
            return self.store.active_registry

    def emit_expiry(
        self, run: JobRecord, context: TaskContext | None, reason: str
    ) -> None:
        """Handles background emission of expiry events for the state stream."""
        ctx = context or TaskContext.create_placeholder(run)
        manifest = None
        path = self.store.resolve_task_path(run.RUN_ID)

        if path and (path / MANIFEST_FILENAME).exists():
            try:
                with (path / MANIFEST_FILENAME).open("rb") as f:
                    manifest = msgspec.json.decode(f.read(), type=TaskManifest)
            except Exception:
                pass

        if not manifest:
            manifest = TaskManifest(
                job_id=run.JOB_ID,
                run_id=run.RUN_ID,
                dataset_id=run.DATASET_ID,
                status=ExecutionStatus.EXPIRED,
                current_stage="N.A",
                remarks=reason,
                bitmask=0,
            )

        self.store._emit_state(
            manifest=manifest,
            context=ctx,
            metadata={"status": ExecutionStatus.EXPIRED, "remarks": reason},
        )
