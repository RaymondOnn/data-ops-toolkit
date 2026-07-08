"""Daemon-specific task recovery and expiration handling."""

from typing import Any

from apps.ingestion.src.core.models.task import ExecutionStatus, Task, TaskSignal
from apps.ingestion.src.core.models.task.enums import TaskIdentity
from apps.ingestion.src.core.orchestrator.common import Janitor
from apps.ingestion.src.core.orchestrator.enums import TaskRecord, TaskRef
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE
from loguru import logger

from .state import DaemonState

LOG = logger


class DaemonJanitor:
    """
    Daemon-specific task recovery and expiration handling.

    Responsibilities:
    - Recover failed tasks from quarantine
    - Process expired runs (clean up stale records)
    """

    def __init__(self, janitor: Janitor, state: DaemonState):
        self.janitor = janitor
        self.state = state
        self.exec_ctx = janitor.exec_ctx

    def evict_expired_run(
        self, record: TaskRecord, task_context: Any | None = None
    ) -> None:
        """
        Evict an expired run from state and clean up artifacts.

        Handles both:
        - Triggered runs (have workspace folders)
        - Untriggered runs (only database records)
        """
        run_id = record.RUN_ID
        triggered = record.is_triggered
        reason = "TTL_EXPIRED" if triggered else "UNTRIGGERED_STALE"
        full_reason = f"{reason} | Scheduled: {record.SCHEDULED_TIMESTAMP_LC}"

        LOG.debug(f"Evicting run {run_id}: {full_reason}")

        # Log expiry to state stream
        self.state.log_expiry(run=record, context=task_context, reason=full_reason)

        if triggered:
            self._evict_triggered_run(record, run_id, full_reason)
        else:
            self._evict_untriggered_run(record, run_id)

        # Remove from registry
        self.state.hub.remove_task(run_id)

    def _evict_triggered_run(
        self, record: TaskRecord, run_id: str, reason: str
    ) -> None:
        """Evict a run that has been triggered (has workspace)."""
        identity = TaskIdentity(
            job_id=record.JOB_ID,
            dataset_id=record.DATASET_ID,
            partition_date=str(record.PARTITION_DATE or ""),
            run_id=run_id,
        )

        task = Task(
            task_ref=TaskRef(
                namespace=CACHE_TASK_NAMESPACE,
                status=ExecutionStatus(record.JOB_STATUS),
                stage=record.CURRENT_STAGE or "UNKNOWN",
                identity=identity,
            ),
            worker_id="janitor-expiry",
            exec_ctx=self.exec_ctx,
        )

        LOG.warning(f"Task TTL exceeded for {run_id}: {reason}")
        task.update_manifest({"status": ExecutionStatus.EXPIRED.value})
        task.send_signal(TaskSignal.EXPIRED)
        self.janitor.cleanup_task(task)

    def _evict_untriggered_run(self, record: TaskRecord, run_id: str) -> None:
        """Evict a run that was never triggered (no workspace)."""
        identity = TaskIdentity(
            job_id=record.JOB_ID,
            dataset_id=record.DATASET_ID,
            partition_date=str(record.PARTITION_DATE or ""),
            run_id=run_id,
        )
        self.janitor.remove_orphaned_config(identity.task_key, run_id)
