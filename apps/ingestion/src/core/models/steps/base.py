import shutil
import time
from abc import ABC, abstractmethod
from enum import IntEnum, IntFlag, auto
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec
import structlog
from src.core.models.job import JobStatus
from src.core.models.job.manifest import AuditPayload, JobManifest
from src.utils.constants import JOB_STEPS_BASE_DIR

from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitBreakerTripped

if TYPE_CHECKING:
    from src.core.models.job import Job

LOG = structlog.getLogger(__name__)


class JobBitmask(IntFlag):
    """
    Class representing the bitmask for job steps.
    """

    NONE = 0
    START = auto()
    RAW = auto()
    TRANSFORM = auto()
    WRITE = auto()
    AUDIT = auto()
    PUBLISH = auto()
    COMPLETE = auto()

    @classmethod
    def ALL_DONE(cls) -> "JobBitmask":
        """
        Dynamically calculates the sum of all flags.
        Useful for checking if the 50M row pipeline is 100% complete.
        """
        mask = cls.NONE
        for member in cls:
            mask |= member
        return mask

    def is_fully_complete(self) -> bool:
        """Helper to check if the current instance matches ALL_DONE."""
        return self == self.ALL_DONE()


class JobSteps(IntEnum):
    START = 0
    RAW = 1
    TRANSFORM = 2
    WRITE = 3
    AUDIT = 4
    PUBLISH = 5
    COMPLETE = 6

    @property
    def label(self) -> str:
        return self.name.casefold()

    @property
    def bitmask(self) -> JobBitmask:
        # Map the step to the IntFlag
        mapping = {
            JobSteps.START: JobBitmask.START,
            JobSteps.RAW: JobBitmask.RAW,
            JobSteps.TRANSFORM: JobBitmask.TRANSFORM,
            JobSteps.WRITE: JobBitmask.WRITE,
            JobSteps.AUDIT: JobBitmask.AUDIT,
            JobSteps.PUBLISH: JobBitmask.PUBLISH,
            JobSteps.COMPLETE: JobBitmask.COMPLETE,
        }
        return mapping[self]

    @classmethod
    def next_step(cls, current_label: str) -> "JobSteps" | None:
        """Finds the next step in the sequence based on a string label."""
        current_enum = cls[current_label.upper()]
        try:
            return cls(current_enum.value + 1)
        except ValueError:
            return None  # We have reached the end of the pipeline

    @classmethod
    def prev_step(cls, current_label: str) -> "JobSteps" | None:
        """Finds the next step in the sequence based on a string label."""
        current_enum = cls[current_label.upper()]
        try:
            return cls(current_enum.value - 1)
        except ValueError:
            return None  # We have reached the start of the pipeline


_JOB_ORDER = [step.label for step in sorted(JobSteps)]


class JobStep(ABC):
    """Base class for JobStage classes."""

    def __init__(self, step: JobSteps) -> None:
        self.name = step.label
        self.bitmask = step.bitmask

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
        next_step = JobSteps.next_step(self.name)
        if next_step:
            job.set_step(JobStep.get_step_class_by_name(next_step.label))
            return next_step.label
        return "FINISH"

    def move_to_folder(self, job: "Job", category: str) -> None:
        """
        Physically moves the metadata folder to HOLD or QUARANTINE.
        category: "HOLD" | "QUARANTINE" | "DONE"
        """
        base_dir = Path(JOB_STEPS_BASE_DIR)

        # Target: base/HOLD/job_id/run_id
        new_path = base_dir / category / f"{job.id}_{job.run_id}"

        # Ensure parent structure exists
        new_path.parent.mkdir(parents=True, exist_ok=True)

        if job.folder.exists():
            LOG.info("Moving metadata folder", src=job.folder, dst=new_path)
            # shutil.move handles cross-filesystem moves if necessary
            shutil.move(str(job.folder), str(new_path))

            # Update the job instance reference so subsequent saves hit the new path
            job.folder = new_path

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
        data = msgspec.to_builtins(job.manifest)

        # 2. MUTATE (same as before)
        if exception:
            # 2. ROUTING LOGIC (The "Sorting Hat")
            if isinstance(exception, (CircuitBreakerTripped, ClientCantConnect)):
                # If we fail during CompleteStep, it's Deferred (Ready to wrap up)
                # Otherwise, it's Blocked (Needs to re-run current step)
                # data["job_status"] = (
                #     JobStatus.DEFERRED if self.name == "CompleteStep"
                #     else JobStatus.BLOCKED
                # )
                from src.core.models.states.terminal import HoldState

                HoldState(job).on_enter(exception)
                target_category = "HOLD"
            else:
                from src.core.models.states.terminal import FailedState

                FailedState(job).on_enter(exception)
                target_category = "FAILED"

            self.move_to_folder(job, target_category)
        else:
            current_mask = data["bitmask"]
            new_mask = current_mask | self.bitmask
            if new_mask.is_fully_complete():
                from src.core.models.states.terminal import SuccessState

                SuccessState(job).on_enter()
                data["job_status"] = JobStatus.SUCCESS

            if results:
                data[self.name] = results

        # 4. SYMLINK (Pointer to immutable data)
        if data_folder:
            active_link = job.folder / self.name
            if active_link.exists() or active_link.is_symlink():
                active_link.unlink()

            # Pointer: active/job_id/run_id/step -> ../../../data/step/folder
            relative_target = (
                Path("..") / ".." / ".." / "data" / self.name / data_folder.name
            )
            active_link.symlink_to(relative_target)

        # 5. ATOMIC SWAP
        job.request_status_sync()

    @classmethod
    def get_step_class_by_name(cls, name: str) -> "JobStep":
        """
        Given a step name, returns the corresponding JobStep class.

        Iterates through all subclasses of JobStep and checks if the name attribute matches the given name.
        If no match is found, raises a ValueError.
        """
        for cls in cls.__subclasses__():
            # If you have nested subclasses, you may want a recursive walk here.
            if getattr(cls, "name", None) == name:
                idx = _JOB_ORDER.index(name)
                step = JobSteps(idx)
                return cls(step=step)
        raise ValueError(f"Unknown step name: {name}")

    def get_manifest(self, job: "Job") -> JobManifest:
        """Helper to read the current state of the world."""
        if job.manifest_path and not job.manifest_path.exists():
            raise FileNotFoundError(f"Manifest missing at {job.manifest_path}")

        with open(job.manifest_path, "rb") as f:
            return msgspec.json.decode(f.read(), type=JobManifest)


class AuditStep(JobStep):
    manifest: AuditPayload

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
