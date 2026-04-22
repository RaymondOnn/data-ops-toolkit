import shutil
from pathlib import Path

import msgspec
from core.orchestrator.manager import TaskManger
from loguru import logger

from apps.ingestion.src.core.contexts import ExecutionContext, TaskContext
from apps.ingestion.src.core.models.states.terminal import HoldState
from apps.ingestion.src.core.models.task import ExecutionStatus, Task, TaskSignal
from apps.ingestion.src.core.orchestrator.state import StateStore
from apps.ingestion.src.utils.common import find_path
from apps.ingestion.src.utils.constants import CONFIG_FILENAME, MANIFEST_FILENAME
from apps.ingestion.src.utils.dates import is_expired

LOG = logger


class LifecycleManager:
    def __init__(
        self,
        state_store: StateStore,
        engine: TaskManger,
        exec_ctx: ExecutionContext,
    ) -> None:
        self.state_store = state_store
        self.engine = engine
        self.exec_ctx = exec_ctx

    def handle_recovery(self) -> None:
        """
        Scans the HOLD and FAILED directories to re-queue stuck jobs.
        Uses the directory structure as the source of truth when the DB is stale.
        """
        LOG.info("Starting recovery sweep for HOLD and FAILED states")

        for category in ["HOLD", "FAILED"]:
            base_path = self.exec_ctx.workspace_dir / category
            if not base_path.exists():
                continue

            # rglob finds all manifests regardless of how deep the job/run IDs are nested
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

        # Final flush to Postgres to commit all recovered statuses
        self.state_store.flush()
        LOG.info("Recovery sweep complete.")

    def recover_task_by_path(self, folder_path: Path) -> None:
        """
        Helper to recover a single task given its directory.
        """
        category = folder_path.parent.parent.name

        try:
            # 1. Rehydrate the Task object from its current location
            task = Task.from_folder(folder_path, exec_ctx=self.exec_ctx)

            # 2. Resumption Logic Check
            if category == "HOLD":
                state = HoldState(task)
                if not state.can_recover():
                    LOG.warning("Task in HOLD cannot recover yet", run_id=task.run_id)
                    return

            LOG.info(
                f"Recovering job from {category}",
                run_id=task.run_id,
                stage=task.manifest.current_stage,
            )

            # 3. Move back to 'active' folder (Atomic handover)
            task.move_to_folder("active")

            # 4. Reset manifest status
            # If we are recovering from FAILED, we clear the current stage's data to be safe
            stage_to_reset = (
                task.manifest.current_stage if category == "FAILED" else None
            )
            task.reset_for_retry(stage_to_clear=stage_to_reset)

            if category == "FAILED":
                task.update_manifest({"retry_count": task.manifest.retry_count + 1})

            # 5. Re-queue into the Ingestion Engine
            # Use standardized config name 'config.json'
            self.engine.queue_tasks(
                identifier=task.id,
                run_id=task.run_id,
                config_file_path=str(task.folder / CONFIG_FILENAME),
                current_stage=task.manifest.current_stage,
            )

            # 6. Signal the state change to observers (Heartbeat)
            task.request_status_sync(TaskSignal.SYNC)
            self.state_store.flush()
        except Exception:
            LOG.exception("Recovery failed", path=str(folder_path))
            raise

    def handle_expiry(self) -> None:
        """
        Scans for jobs that have passed their TTL and purges their workspaces.
        """
        # List to prevent 'dictionary changed size during iteration'
        runs_to_check = list(self.state_store.active_records.values())

        for run in runs_to_check:
            # We only expire jobs that are stuck in a non-terminal state
            status = run.JOB_STATUS
            if status and status in ExecutionStatus.active_statuses():
                try:
                    job_id = run.JOB_ID
                    run_id = run.RUN_ID
                    dataset_id = run.DATASET_ID
                    partition_date = str(run.PARTITION_DATE)

                    if not all([job_id, run_id, dataset_id, partition_date]):
                        continue

                    # Standardize path resolution from database metadata
                    job_path = self.exec_ctx.get_run_path(
                        job_id, dataset_id, partition_date, run_id
                    )
                    if not job_path.exists():
                        continue

                    # Load only the config/context (fast)
                    ctx = self.get_context_from_path(job_path)

                    if is_expired(ctx.expires_at):
                        LOG.warning(
                            "Task TTL reached. Initiating purge.",
                            job_id=job_id,
                            run_id=run_id,
                        )

                        # 1. Perform physical cleanup
                        self._cleanup_workspace(run_id)

                        # 2. Update State Store to terminal status
                        self.state_store.update_run(
                            run_id,
                            {"status": ExecutionStatus.EXPIRED, "stage": "cleanup"},
                        )

                except (OSError, msgspec.DecodeError) as e:
                    LOG.error(
                        "Expiry check failed due to IO or malformed config",
                        run_id=run.RUN_ID,
                        error=str(e),
                    )
                except Exception:
                    LOG.exception(
                        "Unexpected failure during expiry check",
                        run_id=run.RUN_ID,
                    )

        self.state_store.flush()

    def get_context_from_path(self, folder: Path) -> "TaskContext":
        """
        Helper to load the TaskContext from the active workspace.
        Standardized to look for 'config.json' directly.
        """

        # In our refactor, we standardized the filename to config.json
        config_path = folder / CONFIG_FILENAME

        if not config_path.exists():
            # Fallback for legacy naming if necessary, otherwise stick to strict
            raise FileNotFoundError(f"Missing config.json in {folder}")

        with config_path.open(mode="rb") as f:
            # msgspec handles the mapping to TaskContext class automatically
            return msgspec.json.decode(f.read(), type=TaskContext)

    def _cleanup_workspace(self, run_id: str) -> None:
        """
        The 'Janitor' method. Deletes active links and physical data.
        """
        # 1. Remove active links
        # Find the active link for the run_id
        active_root = self.exec_ctx.active_path
        active_path = find_path(active_root, run_id)

        if active_path:
            composite_key, _ = active_path.parent.name.split("_")
            job_id, _ = composite_key.split(":")
            shutil.rmtree(active_path)

        # TODO: This needs updating. Cant remember to data folder structure
        # 2. Remove physical data vaults (extract, transform, etc)
        # Search data/ folders for {job_id}_*
        data_root = self.exec_ctx.data_path
        for stage_dir in data_root.iterdir():
            if stage_dir.is_dir():
                for physical_folder in stage_dir.glob(f"{job_id}_*"):
                    shutil.rmtree(physical_folder)
