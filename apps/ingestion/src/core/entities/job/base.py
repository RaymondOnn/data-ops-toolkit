import logging
from pathlib import Path
from typing import Optional

import msgspec

from src.core.context.job import JobContext
from src.core.entities.job.steps.base import JobStep
from src.core.entities.job.manifest import JobManifest



LOG = logging.getLogger(__name__)


class JobStatus:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Job:
    status: JobStatus
    run_id: str
    _step: JobStep | None = None
    manifest_path: Path | None = None
    
    
    def __init__(
        self,
        job_id: str,
        run_id: str,
        worker_id: str,
        job_context: JobContext,
        start_step: Optional[str] = "start",
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
        if start_step:
            self._step = JobStep.get_step_class_by_name(start_step)
        self.status = JobStatus.STARTING
        self.folder = None
        self.manifest_path = None


    @property
    def step(self) -> JobStep:
        if not self._step:
            raise ValueError("Job is not initialized.")
        return self._step

    def set_step(self, step: JobStep) -> None:
        self._step = step

    def execute(self) -> None:
        if not self._step:
            raise ValueError("Job is not initialized.")

        self._step.execute(self)
    
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
            # Fallback to current step folder if path isn't explicit
            self.manifest_path = self.folder / "manifest.json"

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

