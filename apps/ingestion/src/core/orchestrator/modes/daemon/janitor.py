from typing import Any

from apps.ingestion.src.core.models.task import ExecutionStatus, Task, TaskSignal
from apps.ingestion.src.core.models.task.enums import TaskIdentity
from apps.ingestion.src.core.orchestrator.common import Janitor
from apps.ingestion.src.core.orchestrator.enums import JobRecord, TaskRef
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE
from loguru import logger

from .state import DaemonStateStore

LOG = logger


class DaemonJanitor:
    """
    Facility Management logic used primarily in Daemon mode.
    Handles the 'When' and 'Why' of recovery and eviction.
    """

    def __init__(
        self,
        janitor: Janitor,
        state_monitor: DaemonStateStore,
    ):
        self.janitor = janitor
        self.state_monitor = state_monitor
        self.exec_ctx = janitor.exec_ctx

    def recover_failed_tasks(self) -> None:
        """Scans FAILED and HOLD directories to re-queue stuck jobs."""
        LOG.info("Starting recovery sweep for quarantined states")
        roots = [self.exec_ctx.failed_path, self.exec_ctx.workspace_dir / "HOLD"]
        for folder in self.janitor._discover_task_folders(roots):
            try:
                self.janitor.recover_task_by_path(folder)  # Call on common Janitor
            except Exception:
                LOG.exception(f"Unexpected error recovering {folder}")
        LOG.info("Recovery sweep done.")

    def process_expired_run(self, run: JobRecord, task_ctx: Any | None) -> None:
        """Handles eviction of stale/expired records from the DB and disk."""
        run_id, reason = (
            run.RUN_ID,
            ("TTL_EXPIRED" if run.has_been_triggered else "UNTRIGGERED_STALE"),
        )
        full_reason = f"{reason} | Scheduled: {run.SCHEDULED_TIMESTAMP_LC}"
        LOG.warning(f"Evicting run {run_id} ({full_reason})")

        self.state_monitor.emit_expiry(run=run, context=task_ctx, reason=full_reason)
        if run.has_been_triggered:
            identity = TaskIdentity(
                job_id=run.JOB_ID,
                dataset_id=run.DATASET_ID,
                partition_date=str(run.PARTITION_DATE or ""),
                run_id=run_id,
            )
            task = Task(
                task_ref=TaskRef(
                    namespace=CACHE_TASK_NAMESPACE,
                    status=ExecutionStatus(run.JOB_STATUS),
                    stage=run.CURRENT_STAGE or "UNKNOWN",
                    identity=identity,
                ),
                worker_id="janitor-expiry",
                exec_ctx=self.exec_ctx,
            )
            LOG.warning(f"Task TTL exceeded for {run_id}: {full_reason}")
            task.update_manifest({"status": ExecutionStatus.EXPIRED.value})
            task.request_status_sync(TaskSignal.EXPIRED)
            self.janitor.cleanup_task(task)
        else:
            identity = TaskIdentity(
                job_id=run.JOB_ID,
                dataset_id=run.DATASET_ID,
                partition_date=str(run.PARTITION_DATE or ""),
                run_id=run_id,
            )
            self.janitor._purge_orphaned_config(identity.identifier, run_id)

        self.state_monitor.store.remove_record(run.RUN_ID)
