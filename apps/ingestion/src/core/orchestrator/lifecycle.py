import shutil
import time
from pathlib import Path

import msgspec
import structlog
from src.core.contexts.job import JobContext
from src.core.models.job import Job, JobStatus
from src.core.models.states.terminal import HoldState
from src.core.orchestrator.engine import IngestionEngine
from src.core.orchestrator.state import StateStore
from src.utils.constants import JOB_STEPS_BASE_DIR
from src.utils.dates import is_expired
from src.utils.common import find_path

LOG = structlog.getLogger(__name__)


class LifecycleManager:
    def __init__(self, state_store: StateStore, engine: IngestionEngine) -> None:
        self.state_store = state_store
        self.engine = engine

    def _handle_recovery(self) -> None:
        """
        Scans the HOLD and FAILED directories to re-queue stuck jobs.
        Uses the directory structure as the source of truth when the DB is stale.
        """
        LOG.info("Starting recovery sweep...")
        # We focus on HOLD for auto-resumption
        hold_base = JOB_STEPS_BASE_DIR / "HOLD"
        if not hold_base.exists():
            return

        # rglob finds all manifests regardless of how deep the job/run IDs are nested
        for manifest_path in hold_base.rglob("manifest.json"):
            try:
                # 1. Rehydrate the Job object from the folder metadata
                # Line 204 fix: Using the new classmethod
                job = Job.from_folder(manifest_path.parent)
                state = HoldState(job)

                # 2. Logic check: Should this job be resumed?
                # We use a tool-based approach to check dependencies/locks
                if not state.can_recover():
                    continue

                LOG.info(f"Auto-recovering job {job.run_id} from HOLD.")

                # 3. Construct the composite key for the Engine
                # Line 313 fix: composite_key = "job_id:table"
                composite_key = f"{job.id}:{job.context.dataset_id}"
                LOG.info("Recovering job", run_id=job.run_id, step=job.manifest.current_step)

                # 4. Re-queue into the Ingestion Engine
                # This moves the job back into the active processing queue
                self.engine.queue_jobs(
                    composite_key=composite_key,
                    run_id=job.run_id,
                    config_file_path=str(next(job.folder.glob("*_config.json"))),
                    current_step=job.manifest.current_step,
                )

                # 5. Sync the StateStore mirror so the UI reflects the move
                # Line 341 fix: Ensure the DB knows the job is now RUNNING
                self.state_store.sync_from_folder(job.folder)

            except StopIteration:
                LOG.error(f"Recovery failed for {manifest_path}: Missing _config.json")
            except Exception as e:
                LOG.error(f"Error recovering job at {manifest_path}: {e}")

        # Final flush to Postgres to commit all recovered statuses
        self.state_store.flush()
        LOG.info("Recovery sweep complete.")

    def _handle_expiry(self) -> None:
        """
        Scans for jobs that have passed their TTL and purges their workspaces.
        """
        now = time.time()
        # List to prevent 'dictionary changed size during iteration'
        runs_to_check = list(self.state_store._mirror.values())

        for data in runs_to_check:
            # We only expire jobs that are stuck in a non-terminal state
            if data["status"] in JobStatus.active_statuses():
                try:
                    # Use get_context to check expiry without a full manifest parse
                    job_path = Path(data["folder_path"])
                    if not job_path.exists():
                        continue

                    # Load only the config/context (fast)
                    ctx = self.get_context_from_path(job_path)

                    if is_expired(ctx.expires_at):
                        LOG.warning(
                            "Job TTL reached. Initiating purge.",
                            job_id=data["job_id"],
                            run_id=data["run_id"],
                        )

                        # 1. Perform physical cleanup
                        self._cleanup_workspace(data["job_id"])

                        # 2. Update State Store to terminal status
                        self.state_store.update_run(
                            data["run_id"],
                            {"status": JobStatus.EXPIRED, "step": "cleanup"},
                        )

                except Exception as e:
                    LOG.error("Expiry check failed", run_id=data["run_id"], error=str(e))

        self.state_store.flush()

    def get_context_from_path(self, folder: Path) -> "JobContext":
        """
        Helper to load the JobContext from the active workspace.
        Standardized to look for 'config.json' directly.
        """
        from src.core.contexts.job import JobContext

        # In our refactor, we standardized the filename to config.json
        config_path = folder / "config.json"

        if not config_path.exists():
            # Fallback for legacy naming if necessary, otherwise stick to strict
            raise FileNotFoundError(f"Missing config.json in {folder}")

        with open(config_path, "rb") as f:
            # msgspec handles the mapping to JobContext class automatically
            return msgspec.json.decode(f.read(), type=JobContext)

    def _cleanup_workspace(self, run_id: str) -> None:
        """
        The 'Janitor' method. Deletes active links and physical data.
        """
        # 1. Remove active links
        # Find the active link for the run_id
        active_root = JOB_STEPS_BASE_DIR / "active"
        active_path = find_path(active_root, run_id)

        if active_path:
            composite_key, run_date = active_path.parent.name.split("_")
            job_id, dataset_id = composite_key.split(":")
            shutil.rmtree(active_path)

        # TODO: This needs updating. Cant remember to data folder structure
        # 2. Remove physical data vaults (raw, transform, etc)
        # Search data/ folders for {job_id}_*
        data_root = JOB_STEPS_BASE_DIR / "data"
        for step_dir in data_root.iterdir():
            if step_dir.is_dir():
                for physical_folder in step_dir.glob(f"{job_id}_*"):
                    shutil.rmtree(physical_folder)
