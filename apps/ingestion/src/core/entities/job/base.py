import os
from pathlib import Path
from enum import StrEnum

import msgspec
import structlog

from src.core.context.job import JobContext
from src.core.entities.job.steps.base import JobStep
from src.core.entities.job.manifest import JobManifest



LOG = structlog.getLogger(__name__)

class JobStatus(StrEnum):
    """
    Shows the current health of the job.
    
    Independent of Job Step for easier maintenance
    """
    
    # Initial State
    PENDING = "PENDING"     # Created, waiting for schedule
    QUEUED = "QUEUED"       # Picked up by Orchestrator, waiting for Worker
    
    # Active States
    PREPPING = "PREPPING"   # Worker initialized, manifest created
    RUNNING = "RUNNING"     # Actively processing a step
    
    # Terminal States (End of the road)
    SUCCESS = "SUCCESS"     # Fully finished (Complete step passed)
    FAILED = "FAILED"       # Hard stop, requires manual intervention
    CANCELLED = "CANCELLED" # Manual kill
    EXPIRED = "EXPIRED"     # TTL reached, data purged, no recovery needed
    
    # Wait States (The "Breadcrumb" triggers)
    DEFERRED = "DEFERRED"   # Transient error, Orchestrator will retry later
    BLOCKED = "BLOCKED"     # Manual HOLD or dependency missing

class Job:
    status: JobStatus
    run_id: str
    manifest_path: Path | None = None
    
    
    def __init__(
        self,
        job_id: str,
        run_id: str,
        worker_id: str,
        job_context: JobContext,
        start_step: str = "start",
    ) -> None:
        """
        Initialize a Job instance with the given step.

        :param step: The step name to start from (e.g. "raw", "transform", etc.). If None, the job will start from the beginning.
        :type step: str
        """
        self.id = job_id
        self.job_config = job_context
        self.worker_id = worker_id
        self.run_id = run_id
        self._step = JobStep.get_step_class_by_name(start_step)
        self.manifest = JobManifest(
            job_id=job_id, 
            run_id=run_id,
            job_status="PENDING",
            current_step="START"
        )
        self.folder: Path | None = None
        self.manifest_path = None

    @classmethod
    def from_folder(cls, folder: Path) -> "Job":
        """Rehydrates a Job instance from its physical workspace."""
        # Context is stored as <composite_key>_<run_id>_config.json
        config_files = list(folder.glob("*_config.json"))
        if not config_files:
            raise FileNotFoundError(f"No config found in {folder}")

        with open(config_files[0], "rb") as f:
            ctx = msgspec.json.decode(f.read(), type=JobContext)

        # Re-read manifest for current step/status
        manifest_path = folder / "manifest.json"
        with open(manifest_path, "rb") as f:
            manifest = msgspec.json.decode(f.read(), type=JobManifest)

        return cls(
            job_id=ctx.job_id,
            run_id=manifest.run_id,
            worker_id="recovery",
            job_context=ctx,
            start_step=manifest.current_step
        )
        
    @property
    def step(self) -> JobStep:
        if not self._step:
            raise ValueError("Job is not initialized.")
        return self._step

    def set_step(self, step: JobStep) -> None:
        self._step = step

    def execute(self) -> None:
        """Execute the current job step.

        :raises ValueError: If the job is not initialized.
        """
        if not self.step:
            raise ValueError("Job is not initialized.")

        self.step.execute(job=self)
    
    def get_manifest(self) -> JobManifest:
        """Helper to load the manifest from the current folder."""
        # Note: manifest_path should be set during Step execution or Job init
        if not self.manifest_path or not self.manifest_path.exists():
            # Return a default if not found
            return JobManifest(job_id=self.id, run_id=self.run_id, ...)
        
        with open(self.manifest_path, "rb") as f:
            return msgspec.json.decode(f.read(), type=JobManifest)

    def save_manifest(self) -> None:
        """
        Writes the JobManifest to disk. This is the 'Source of Truth'.
        """
        if not self.manifest_path:
            if not self.folder:
                raise ValueError("Job folder is not set.")
            # Fallback to current step folder if path isn't explicit
            self.manifest_path = Path(self.folder) / "manifest.json"

        # Ensure the parent directory exists
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)

        # Atomic Write: Write to .tmp then rename to avoid corruption during crashes
        tmp_path = self.manifest_path.with_suffix(".tmp")
        with open(tmp_path, "wb") as f:
            f.write(msgspec.json.encode(self.manifest))
            f.flush()
            os.fsync(f.fileno()) # Ensure bits are physically on the platter
        
        tmp_path.replace(self.manifest_path)
            
    def update_status(self) -> None:
        """
        Drops a signal file to notify the Orchestrator of a state change.
        """
        from src.utils.constants import JOB_STEPS_BASE_DIR
        
        # 1. Ensure the manifest is written to disk first
        self.save_manifest() 

        # 2. Define the signal path
        # Path: /data/signals/{run_id}.step_name.bitmask.sync
        signal_dir = JOB_STEPS_BASE_DIR / "signals"
        signal_dir.mkdir(parents=True, exist_ok=True)
        
        # We embed metadata in the filename so the Orchestrator 
        # might not even need to open the manifest for simple status updates.
        temp_path = signal_dir / f".tmp_{self.run_id}.sync"
        final_path = signal_dir / f"{self.run_id}.sync"

        temp_path.touch()              # Create hidden/temp
        temp_path.replace(final_path)  # Atomic switch to visible
        
    def update_status(self):
        """Atomic signal: Save manifest then drop breadcrumb."""
        from src.utils.constants import JOB_STEPS_BASE_DIR
        
        # 1. Write Source of Truth
        if self.folder and not self.manifest_path:
            self.manifest_path = Path(self.folder) / "manifest.json"
        else:
            raise ValueError("Job folder is not set.")
            
        # Atomic write to avoid partial reads by Orchestrator
        tmp_path = self.manifest_path.with_suffix(".tmp")
        with open(tmp_path, "wb") as f:
            f.write(msgspec.json.encode(self.get_manifest()))
        tmp_path.replace(str(self.manifest_path))

        # 2. Drop the Breadcrumb signal
        signal_dir = Path(JOB_STEPS_BASE_DIR) / "signals"
        signal_dir.mkdir(parents=True, exist_ok=True)
        
        # Filename contains run_id for Orchestrator lookup
        crumb_name = f"{self.run_id}.{self.step.name}.sync"
        (signal_dir / crumb_name).touch()

