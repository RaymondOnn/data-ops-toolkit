import time
import traceback
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec # type: ignore
import polars as pl # type: ignore
import structlog # type: ignore

from src.core.models.job.manifest import (
    AuditPayload,
    ErrorPayload,
    JobManifest,
)


from src.utils.exceptions import JobFailed, JobBlocked, JobDeferred


from libs.resilience.circuit_breaker import CircuitBreakerTripped

if TYPE_CHECKING:
    from src.core.models.job import Job

LOG = structlog.getLogger(__name__)
_JOB_ORDER = [
    "start",
    "raw",
    "transform",
    "write",
    "audit",
    "publish",
    "complete",
]


class JobBitmask:
    """
    Class representing the bitmask for job steps.
    """

    START: str = "0000001"
    RAW: str = "0000010"
    TRANSFORM: str = "0000100"
    WRITE: str = "0001000"
    AUDIT: str = "0010000"
    PUBLISH: str = "0100000"
    COMPLETE: str = "1000000"


class JobStep(ABC):
    """Base class for JobStage classes."""

    @property
    def bitmask(self) -> str:
        raise NotImplementedError("Subclasses must implement this property")

    @property
    def name(self) -> str:
        raise NotImplementedError("Subclasses must implement this property")

    def get_step(self, offset: int) -> str:
        idx = _JOB_ORDER.index(self.name)
        if 0 <= idx + offset < len(_JOB_ORDER):
            return _JOB_ORDER[idx + offset]
        else:
            raise ValueError(f"Invalid offset: {offset}")
    
    @abstractmethod
    def execute(self, job: "Job") -> str:
        """Execute the current JobStage with the given engine and dataframe.

        :param engine: The IngestionEngine instance.
        :type engine: IngestionEngine
        :param df: The dataframe to process in this stage.
        :type df: Optional[Any]
        """
        raise NotImplementedError

    def _transit(self, job: "Job") -> str:
        """Transit the Job instance to the next stage."""
        next_step = self.get_step(offset=1)
        job.set_step(JobStep.get_step_class_by_name(next_step))
        return next_step
        return "FINISH"

    def finalize(
        self,
        job: "Job",
        data_folder: Path | None = None,
        results: dict[str, Any] | None = None,
        exception: Exception | None = None,
    ) -> None:
        """
        DECISION: Deterministic Paths & Symlinking.
        We avoid searching for 'latest' folders by using a static symlink 
        at active/{job_id}/{step_name}.
        """
        results = results or {}

        # 1. READ & BOOTSTRAP
        # Check if file exists and has content
        if not job.folder:
            raise ValueError("Job folder is not set")

        manifest_path = job.folder / "manifest.json"
        if manifest_path.exists() and manifest_path.stat().st_size > 0:
            with open(manifest_path, "rb") as f:
                data: dict[str, Any] = msgspec.json.decode(f.read())
        else:
            # File is empty or doesn't exist: Start Phase
            data = {
                "job_id": job.id,
                "run_id": job.run_id,
                "dataset_name": job.context.dataset_name,
                "status": "RUNNING",
                "current_step": "init",
            }

        # 2. MUTATE (same as before)
        if exception:
            error_payload = msgspec.to_builtins(
                ErrorPayload(
                    step=self.name,
                    error_type=type(exception).__name__,
                    message=str(exception),
                    stack_trace=traceback.format_exc(),
                    worker_id=job.worker_id,
                )
            )
            data["status"] = "FAILED"
            data["error"] = error_payload
            
            if isinstance(exception, JobFailed)
        else:
            if data["current_step"] == "complete":
                data["status"] = "COMPLETED"
            data["current_step"] = self.name
            if results:
                data[self.name] = results

        # 3. CREATE SYMLINK
        if data_folder:
            active_link = job.folder / self.name
            if active_link.exists() or active_link.is_symlink():
                active_link.unlink()
            
            # Create the pointer to the immutable physical data
            relative_target = Path("..") / ".." / "data" / self.name / data_folder.name
            active_link.symlink_to(relative_target)
        
        # 4. ATOMIC SWAP
        manifest = msgspec.convert(data, JobManifest)
        job.update_status(manifest)
        

    @classmethod
    def get_step_class_by_name(cls, name: str) -> "JobStep":
        """
        Given a step name, returns the corresponding JobStep class.

        Iterates through all subclasses of JobStep and checks if the name attribute matches the given name.
        If no match is found, raises a ValueError.
        """

        for cls in cls.__subclasses__():
            # If you have nested subclasses, you may want a recursive walk here.
            if (
                getattr(cls, "name", None) == name
                or getattr(cls(), "name", None) == name
            ):
                return cls
        raise ValueError(f"Unknown step name: {name}")
    
    def get_manifest(self, job: "Job") -> JobManifest:
        """Helper to read the current state of the world."""
        if job.manifest_path and not job.manifest_path.exists():
            raise FileNotFoundError(f"Manifest missing at {job.manifest_path}")

        with open(job.manifest_path, "rb") as f:
            return msgspec.json.decode(f.read(), type=JobManifest)
        




class AuditStep(JobStep):
    manifest: AuditPayload

    @property
    def bitmask(self) -> str:
        return JobBitmask.AUDIT

    @property
    def name(self) -> str:
        return "audit"

    def execute(self, job: "Job") -> str:
        start_time = time.perf_counter()

        try:
            manifest: JobManifest = self.get_manifest(job)
            write_meta: AuditPayload = (
                manifest.write
            )  # Access staging info from WriteStep

            # 1. Internal Heuristic Checks (The 'Stand-in' Logic)
            # While the external app is missing, we check basic things:
            # - Did we lose more than 10% of data?
            # - Are there nulls in the Primary Key?
            internal_results = self._run_internal_checks(job, write_meta)

            # 2. External App Mock Hook
            # This is where you will eventually call your validation API
            external_status = "NOT_AVAILABLE"

            # # 1. Construct the Shell Command
            # # We pass the staging artifact (table/path) as an argument to the app
            # cmd = [
            #     "validation-app",
            #     "--source", write_meta.staging_artifact,
            #     "--job-id", job.id,
            #     "--run-id", job.run_id
            # ]

            # # 2. Run the Command
            # # capture_output=True allows us to save the logs into our manifest
            # process = subprocess.run(
            #     cmd,
            #     capture_output=True,
            #     text=True,
            #     check=False  # We handle the error manually to finalize the manifest
            # )

            # 3. Build Payload
            duration_ms = int((time.perf_counter() - start_time) * 1000)

            payload = AuditPayload(
                step_outcome="COMPLETED",
                validation_passed=internal_results["passed"],
                total_checks_run=len(internal_results["checks"]),
                failed_checks=internal_results["failures"],
                external_app_status=external_status,
                audit_duration_ms=duration_ms,
            )

            self.finalize(job, results=payload)

            # If validation fails, we stop the pipeline here!
            if not payload.validation_passed:
                raise ValueError(
                    f"Audit failed for Job {job.id}. See manifest for details."
                )

            return self._transit(job)

        except Exception as e:
            self.finalize(job, exception=e)
            raise

    def _run_internal_checks(
        self, job: "Job", write_meta: AuditPayload
    ) -> dict[str, Any]:
        """Simple baseline checks while the real app is under construction."""
        # Example: Check if rows_affected is 0
        checks = []
        failures = []

        # Check 1: Row count > 0
        checks.append("row_count_not_zero")
        if write_meta.rows_affected == 0:
            failures.append(
                {
                    "check": "row_count_not_zero",
                    "message": "No rows were loaded to staging.",
                }
            )

        return {"passed": len(failures) == 0, "checks": checks, "failures": failures}


