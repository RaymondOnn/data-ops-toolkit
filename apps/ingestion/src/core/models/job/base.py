import os
import shutil
import time
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import msgspec
import structlog
from src.core.context.job import JobContext
from src.core.models.job.manifest import JobManifest
from src.core.models.steps import JobStep
from src.utils.constants import JOB_STEPS_BASE_DIR

LOG = structlog.getLogger(__name__)
if TYPE_CHECKING:
    from src.core.models.steps import JobSteps




class JobStatus(StrEnum):
    """
    Shows the current health of the job.

    Independent of Job Step for easier maintenance
    """

    # Initial State
    PENDING = "PENDING"  # Created, waiting for schedule
    QUEUED = "QUEUED"  # Picked up by Orchestrator, waiting for Worker

    # Active States
    PROVISIONING = "PROVISIONING"  # Worker initialized, manifest created
    RUNNING = "RUNNING"  # Actively processing a step

    # Terminal States (End of the road)
    SUCCESS = "SUCCESS"  # Fully finished (Complete step passed)
    FAILED = "FAILED"  # Hard stop, requires manual intervention
    CANCELLED = "CANCELLED"  # Manual kill
    EXPIRED = "EXPIRED"  # TTL reached, data purged, no recovery needed

    # Wait States (The "Breadcrumb" triggers)
    DEFERRED = "DEFERRED"  # Transient error, Orchestrator will retry later
    BLOCKED = "BLOCKED"  # Manual HOLD or dependency missing

    @classmethod
    def active_statuses(cls) -> set:
        return {cls.QUEUED, cls.PENDING, cls.RUNNING, cls.PROVISIONING}

    @classmethod
    def terminal_statuses(cls) -> set:
        return {cls.SUCCESS, cls.FAILED, cls.CANCELLED}


# TODO: Rename folders to include worker id?
class Job:
    _step: JobSteps
    _folder: Path
    _manifest_path: Path
    
    def __init__(
        self,
        job_id: str,
        run_id: str,
        worker_id: str,
        target_step: str = JobSteps.START.name,
    ) -> None:
        self.run_id = run_id
        self.id = job_id
        self.dataset = None
        self.worker_id = worker_id
        self.target_step = target_step
        

    @property
    def folder(self) -> Path:
        if not self._folder:
            # 1. Setup the Active Directory
            # Path: storage/active/{job_id}
            folder = JOB_STEPS_BASE_DIR / "active" / f"{self.id}_{self.run_id}"
            folder.mkdir(parents=True, exist_ok=True)

            # 2. Store the initial manifest directly in the active root
            # Decision: The manifest in the active root is the 'Single Source of Truth'
            # for the Orchestrator to monitor progress.
            manifest_path = folder / "manifest.json"
            if not manifest_path.exists():
                manifest_path.touch()

            data = {
                "job_id": self.id,
                "run_id": self.run_id,
                "dataset_name": self.context.dataset_name,  # !: check attribute
                "status": "RUNNING",
                "current_step": self.step.name,
                "bitmask": 0,
            }
            self.update_manifest(data)

            # Move file into job folder
            job_cfg_file = f"{self.id}:{self.context.table}_{self.run_id}_config.json"
            source_path = JOB_STEPS_BASE_DIR / "active" / job_cfg_file
            dest_path = folder / job_cfg_file
            shutil.move(str(source_path), str(dest_path))

            # 4. Update the job pointer
            self._folder = folder
            self._manifest_path = manifest_path
        return Path(self._folder)

    @property
    def manifest(self) -> JobManifest:
        """
        Dynamic accessor. Reads manifest from disk on demand.
        Ensures we don't hold JSON objects for thousands of jobs in RAM.
        """
        if not self._manifest_path.exists():
            # Return a default manifest if file is missing/corrupt
            return JobManifest(job_id=self.id, run_id=self.run_id, status="UNKNOWN")

        with open(self._manifest_path, "rb") as f:
            return msgspec.json.decode(f.read(), type=JobManifest)

    @property
    def context(self) -> JobContext:
        """
        Finds the config file and returns a hydrated JobContext object.
        Does not store the object in self to save RAM.
        """
        try:
            # Look for the first file ending in _config.json
            config_path = next(self.folder.glob("*_config.json"))
            with open(config_path, "rb") as f:
                # msgspec decodes directly into your JobContext class
                return msgspec.json.decode(f.read(), type=JobContext)
        except (StopIteration, FileNotFoundError):
            LOG.error(
                "JobContext configuration missing on disk", folder=str(self.folder)
            )
            # Return an empty/default context if appropriate for your logic
            raise FileNotFoundError(
                f"Config for job {self.id} not found in {self.folder}"
            )

    @property
    def step(self) -> JobStep:
        if not self._step:
            step = self.manifest.current_step or "start"
            JobStep.get_step_class_by_name(step)
        return self._step

    def execute(self) -> None:
        """Execute the current job step.

        :raises ValueError: If the job is not initialized.
        """
        self.step.execute(job=self)

    def update_manifest(self, updates: dict[str, Any] | None = None) -> None:
        """
        Performs an atomic partial update directly to the disk.
        """
        updates = updates or {}

        # 1. Read current state
        data = msgspec.to_builtins(self.manifest)

        # 2. Apply updates
        data.update(updates)

        # 3. Atomic Write to avoid corruption during crashes (Write to .tmp then replace)
        tmp_path = self._manifest_path.with_suffix(".tmp")
        with open(tmp_path, "wb") as f:
            f.write(msgspec.json.encode(data))
            f.flush()
            os.fsync(f.fileno())  # Ensure bits are physically on the platter

        tmp_path.replace(self._manifest_path)

    def check_in(self, step_name: str) -> None:
        """
        The 'Step Check-in': Mark the start of a process on disk immediately.
        Ensures the folder reflects the current step if a crash/outage occurs.
        """
        self.update_manifest(
            {"current_step": step_name, "status": "RUNNING", "last_active": time.time()}
        )
        LOG.debug("Job checked in to step", run_id=self.run_id, step=step_name)

    def move_to_folder(self, step: str) -> None:
        """
        Physically relocates the metadata folder (active -> HOLD/FAILED).
        Because data is in /data/ vault via symlinks, this move is instant.
        """
        # Target: e.g., /opt/app/steps/HOLD/123/run_abc
        new_path = Path(JOB_STEPS_BASE_DIR) / step / self.id / self.run_id
        new_path.parent.mkdir(parents=True, exist_ok=True)

        if self.folder.exists():
            LOG.info(
                "Relocating metadata folder", src=str(self.folder), dst=str(new_path)
            )

            # Atomic move across the filesystem
            shutil.move(str(self.folder), str(new_path))

            # Update internal references so further updates hit the new home
            self._folder = new_path
            self._manifest_path = new_path / "manifest.json"

    def request_status_sync(self, deep_sync: bool = False) -> None:
        """
        Drops a signal file to notify the Orchestrator of a state change.
        """
        from src.utils.constants import JOB_STEPS_BASE_DIR

        # 2. Define the signal path
        # Path: /data/signals/{run_id}.step_name.bitmask.sync
        signal_dir = JOB_STEPS_BASE_DIR / "signals"
        signal_dir.mkdir(parents=True, exist_ok=True)

        # 2. Drop the Breadcrumb
        # The 'Light' signal for progress steps
        ext = ".sync"
        if deep_sync:
            # The 'Heavy' signal for CompleteStep
            ext = ".done"

        # We embed metadata in the filename so the Orchestrator
        # might not even need to open the manifest for simple status updates.
        # Filename contains run_id for Orchestrator lookup
        signal_path = signal_dir / f"{self.run_id}{ext}"
        signal_path.touch()  # Create hidden/temp

    @classmethod
    def from_folder(cls, folder_path: Path, target_step: Optional[JobSteps] = None) -> "Job":
        """
        Factory to rehydrate a Job. If a target_step is provided,
        it performs an immediate check-in.
        """
        folder_name = Path(folder_path).name
        job_id, run_id = folder_name.split("_")

        instance = cls(
            job_id=job_id,
            run_id=run_id,
            worker_id="recovery",
            target_step=target_step,
        )
        if target_step:
            instance.check_in(target_step)
        return instance

    # TODO: Consider if this is needed
    # @property
    # def progress_report(self) -> dict[str, str]:
    #     """
    #     Returns a human-readable checklist of job progress.
    #     Example: {"ingest": "DONE", "transform": "PENDING", "complete": "DONE"}
    #     """
    #     current_mask = self.manifest.bitmask
    #     report = {}

    #     # Iterate through our ordered enum to build the checklist
    #     for step in StepOrder:
    #         # Check if the specific bit for this step is flipped
    #         is_done = bool(current_mask & step.bitmask_flag)
    #         report[step.label] = "DONE" if is_done else "PENDING"

    #     return report
