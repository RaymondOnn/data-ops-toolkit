from pathlib import Path

import msgspec
from apps.ingestion.src.core.contexts import ExecutionContext, TaskContext
from apps.ingestion.src.core.contexts.task import load_task_context
from apps.ingestion.src.core.models.task import ExecutionStatus, Task, TaskSignal
from apps.ingestion.src.core.strategies.cleanup.cleanup import CleanupCoordinator
from apps.ingestion.src.utils.constants import CONFIG_FILENAME, MANIFEST_FILENAME
from loguru import logger

from .enums import JobRecord
from .manager import TaskManager
from .state import StateStore

LOG = logger


class Janitor:
    def __init__(
        self,
        state_store: StateStore,
        tasks: TaskManager,
        exec_ctx: ExecutionContext,
    ) -> None:
        self.state_store = state_store
        self.tasks = tasks
        self.exec_ctx = exec_ctx

    def recover_failed_tasks(self) -> None:
        """
        Scans the FAILED directory to re-queue stuck jobs.
        Uses the directory structure as the source of truth when the DB is stale.
        """
        LOG.info("Starting recovery sweep for FAILED states")

        for category in ["FAILED"]:
            base_path = self.exec_ctx.workspace_dir / category
            if not base_path.exists():
                continue

            for manifest_path in base_path.rglob(MANIFEST_FILENAME):
                try:
                    self.recover_task_by_path(manifest_path.parent)
                except StopIteration:
                    LOG.error(
                        "Recovery failed: Missing config", path=str(manifest_path)
                    )
                except Exception:
                    LOG.exception(
                        "Unexpected error recovering job", path=str(manifest_path)
                    )

        LOG.info("Recovery sweep done.")

    def recover_task_by_path(self, folder_path: Path) -> None:
        """
        Helper to recover a single task given its directory.
        """
        # Workspace structure: {workspace}/{category}/{identifier}/{run_id}
        category = folder_path.parent.parent.name.upper()

        try:
            task = Task.from_folder(folder_path, exec_ctx=self.exec_ctx)
            current_stage = task.manifest.current_stage

            LOG.info(
                f"Recovering task from {category}",
                run_id=task.run_id,
                stage=current_stage,
            )

            # 1. Reset state (status PENDING triggers pick-up by Orchestrator)
            updates = {
                "status": ExecutionStatus.PENDING,
                "current_stage": current_stage,
            }

            if category == "FAILED":
                # Increment attempts and wipe the slate for the stage that failed
                updates["retry_count"] = task.manifest.retry_count + 1
                updates[current_stage] = None

            # 2. Persist state and physically move back to 'active'
            task.update_manifest(updates)
            task.move_to_folder("active")

            # 3. Re-queue into the Hot Cache (TaskManager)
            self.tasks.queue_tasks(
                identifier=task.id,
                run_id=task.run_id,
                config_file_path=str(task.folder / CONFIG_FILENAME),
                current_stage=current_stage,
            )

            # 4. Signal the state change (Sync state with DB)
            task.request_status_sync(TaskSignal.SYNC)
        except Exception:
            LOG.exception("Recovery failed", path=str(folder_path))
            raise

    def process_expired_run(self, run: JobRecord, task_ctx: TaskContext | None) -> None:
        """
        Encapsulates the terminal cleanup and state emission for a single expired run.
        Called by the TriggerManager.
        """
        # 1. Identity & Path Resolution
        run_id = run.RUN_ID
        identifier = self.exec_ctx.get_task_identifier(
            run.JOB_ID, run.DATASET_ID, str(run.PARTITION_DATE) or ""
        )
        job_path = self.exec_ctx.get_run_path(
            run.JOB_ID, run.DATASET_ID, str(run.PARTITION_DATE) or "", run.RUN_ID
        )
        reason = "TTL_EXPIRED" if run.has_been_triggered else "UNTRIGGERED_STALE"
        full_reason = f"{reason} | Scheduled: {run.SCHEDULED_TIMESTAMP_LC}"
        LOG.warning(f"Evicting run {run_id} ({full_reason})")

        # 2. Physical Cleanup
        if run.has_been_triggered:
            task = Task(
                run_id=run_id,
                composite_key=f"{run.JOB_ID}:{run.DATASET_ID}",
                partition_date=str(run.PARTITION_DATE or ""),
                worker_id="janitor",
                exec_ctx=self.exec_ctx,
            )
            self.cleanup_task(task)

        # Defensive: Always check for the orphaned config in the root
        self._purge_orphaned_config(identifier, run_id)

        # 5. Atomic State Transition & Eviction
        # We emit the terminal state (Queuing for flush) and pop from registry
        self.state_store.emit_expiry(run, task_ctx, full_reason)
        self.state_store.remove_record(run.RUN_ID)

    def _get_expiry_context(self, job_path: Path, pending_config: Path) -> TaskContext:
        """Attempts to retrieve TaskContext from disk locations."""
        if job_path.exists():
            return load_task_context(job_path)
        if pending_config.exists():
            with pending_config.open("rb") as f:
                return msgspec.json.decode(f.read(), type=TaskContext)
        LOG.debug("TaskContext not found for expiry check", run_id=job_path.name)
        raise FileNotFoundError("Unable to locate config file")

    def cleanup_task(self, task: Task) -> None:
        """
        Standardizes terminal cleanup of a task using the CleanupCoordinator.
        """
        LOG.info("Janitor: Purging task artifacts", run_id=task.run_id)

        # 1. Coordinate data and metadata cleanup via policies (respects regression_mode)
        CleanupCoordinator().apply(task)

        # 2. Remove legacy orphaned config file in active root
        self._purge_orphaned_config(task.id, task.run_id)

    def _purge_orphaned_config(self, task_id: str, run_id: str) -> None:
        """Removes the legacy orphaned config file in the active root if it exists."""
        prefix = f"{task_id}:{run_id}"
        orphaned_config = self.exec_ctx.active_path / f"{prefix}_{CONFIG_FILENAME}"
        if orphaned_config.exists():
            orphaned_config.unlink()
            LOG.debug("Purged orphaned config file", file=orphaned_config.name)


# TODO: Can we add a 'dry_run' flag to the Janitor class to allow simulating expiry sweeps without deleting files?
# TODO: How can we implement a 'dry_run' flag in the Janitor class that logs physical purges without deleting files?
# TODO: How can we implement a 'DRY_RUN' flag in the Janitor class so I can verify these eviction rules in the logs without actually deleting data?
