import shutil
import time
from abc import ABC
from typing import TYPE_CHECKING

import structlog
from src.core.models.job.steps import JobStep
from src.core.state.base import StateStore
from src.utils.constants import JOB_STEPS_BASE_DIR

if TYPE_CHECKING:
    from src.core.models.job import Job

LOG = structlog.getLogger(__name__)


class TerminalStep(JobStep, ABC):
    """Base for steps that end the active pipeline journey."""

    def park_folder(self, job: "Job", folder_name: str) -> None:
        """Moves folder from active lane to HOLD or QUARANTINE."""
        # Standardize path: data/HOLD/job_id/run_id/
        target_dir = JOB_STEPS_BASE_DIR / self.name / job.id / job.run_id
        target_dir.parent.mkdir(parents=True, exist_ok=True)

        if job.folder.exists():
            shutil.move(str(job.folder), str(target_dir))
            # Update the job object's internal pointer to the new location
            job.folder = target_dir
            job.manifest_path = target_dir / "manifest.json"

    def recover(self, job: "Job", state_store: "StateStore") -> None:
        """Moves folder back to active and resets the Postgres clock."""
        manifest = self.get_manifest(job)

        # Determine where to go back to (failed step or current)
        reentry = (
            manifest.last_error.get("step")
            if manifest.status == "FAILED"
            else self.name
        )

        active_path = JOB_STEPS_BASE_DIR / reentry / job.id / job.run_id
        active_path.parent.mkdir(parents=True, exist_ok=True)

        # 1. Physical Move back to active lane
        shutil.move(str(job.folder), str(active_path))
        job.folder = active_path

        # 2. Reset Manifest Status
        manifest.status = "READY"
        # 3. RESET THE CLOCK: This satisfies the Postgres Misfire Grace Period
        manifest.next_scheduled_time = time.time()

        job.save_manifest(manifest)
        state_store.update_run(manifest)
        state_store.flush()  # Commit to Postgres immediately


class QuarantineStep(TerminalStep):
    @property
    def name(self) -> str:
        return "QUARANTINE"

    def execute(self, job: "Job", error: Exception) -> str:
        # Log the error into the manifest
        self.finalize(job, exception=error)
        # Park it
        self.park_folder(job, "QUARANTINE")
        return "FAILED"


class HoldStep(TerminalStep):
    @property
    def name(self) -> str:
        return "HOLD"

    def execute(self, job: "Job", reason: str) -> str:
        manifest = self.get_manifest(job)
        manifest.status = "HELD"
        manifest.hold_reason = reason
        job.save_manifest(manifest)

        self.park_folder(job, "HOLD")
        return "HELD"

    def check_and_resume(self, job: "Job", state_store: "StateStore") -> bool:
        """Called by Orchestrator preamble in Always-On or Dumb mode."""
        manifest = self.get_manifest(job)

        # Example: If it was waiting for S3, ping S3
        if manifest.hold_reason == "S3_UNAVAILABLE":
            if self._ping_s3():
                self.recover(job, state_store)
                return True
        return False
