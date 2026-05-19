from collections.abc import Callable
from pathlib import Path
from typing import Any

from apps.ingestion.src.core.models.states import ExpiredState
from apps.ingestion.src.core.models.task import ExecutionStatus, Task, TaskSignal
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE, CONFIG_FILENAME
from loguru import logger

from ...common import Janitor, TaskManager
from ...enums import JobRecord, TaskRef
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
        queue_task_fn: Callable[[TaskRef, str], Any],
        active_tasks_fn: Callable[[], dict]
    ):
        self.janitor = janitor
        self.state_monitor = state_monitor
        self.queue_task_fn = queue_task_fn
        self.exec_ctx = janitor.exec_ctx

    def recover_failed_tasks(self) -> None:
        """Scans FAILED and HOLD directories to re-queue stuck jobs."""
        LOG.info("Starting recovery sweep for quarantined states")
        roots = [self.exec_ctx.failed_path, self.exec_ctx.workspace_dir / "HOLD"]
        for folder in self.janitor._discover_task_folders(roots):
            try:
                self.recover_task_by_path(folder)
            except Exception:
                LOG.exception(f"Unexpected error recovering {folder}")
        LOG.info("Recovery sweep done.")

    def recover_task_by_path(self, folder_path: Path) -> None:
        """Helper to recover a single task given its directory."""
        category = folder_path.parent.parent.name.upper()
        try:
            task = Task.from_folder(folder_path, exec_ctx=self.exec_ctx)
            current_stage = task.manifest.current_stage
            LOG.info(
                f"Recovering {task.run_id} from {category} (Stage: {current_stage})"
            )

            updates = {
                "status": ExecutionStatus.PENDING,
                "current_stage": current_stage,
            }
            if category == "FAILED":
                updates["retry_count"] = task.manifest.retry_count + 1
                updates[current_stage] = None

            task.update_manifest(updates)
            task.move_to_folder("active")
            self.queue_task_fn(task.task_ref, str(task.folder / CONFIG_FILENAME))
            task.request_status_sync(TaskSignal.SYNC)
        except Exception:
            LOG.exception("Recovery failed", path=str(folder_path))

    def process_expired_run(self, run: JobRecord, task_ctx: Any | None) -> None:
        """Handles eviction of stale/expired records from the DB and disk."""
        run_id, reason = run.RUN_ID, (
            "TTL_EXPIRED" if run.has_been_triggered else "UNTRIGGERED_STALE"
        )
        full_reason = f"{reason} | Scheduled: {run.SCHEDULED_TIMESTAMP_LC}"
        LOG.warning(f"Evicting run {run_id} ({full_reason})")

        self.state_monitor.emit_expiry(run=run, context=task_ctx, reason=full_reason)
        if run.has_been_triggered:
            task = Task(
                task_ref=TaskRef(
                    namespace=CACHE_TASK_NAMESPACE,
                    status=run.JOB_STATUS,
                    stage=run.CURRENT_STAGE or "UNKNOWN",
                    job_id=run.JOB_ID,
                    dataset_id=run.DATASET_ID,
                    partition_date=str(run.PARTITION_DATE or ""),
                    run_id=run_id,
                ),
                worker_id="janitor-expiry",
                exec_ctx=self.exec_ctx,
            )
            ExpiredState().on_enter(task, data={"reason": full_reason})
            self.janitor.cleanup_task(task)
        else:
            ident = self.exec_ctx.get_task_identifier(
                run.JOB_ID, run.DATASET_ID, str(run.PARTITION_DATE or "")
            )
            self.janitor._purge_orphaned_config(ident, run_id)

        self.state_monitor.store.remove_record(run.RUN_ID)
