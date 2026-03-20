import shutil
import time
import traceback
from abc import ABC, abstractmethod
from enum import IntEnum, IntFlag, auto
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec
import structlog
from src.core.models.job.manifest import ErrorPayload
from src.core.models.states.terminal import FailedState, HoldState, SuccessState

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
    EXTRACT = auto()
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
    EXTRACT = 1
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
            JobSteps.EXTRACT: JobBitmask.EXTRACT,
            JobSteps.TRANSFORM: JobBitmask.TRANSFORM,
            JobSteps.WRITE: JobBitmask.WRITE,
            JobSteps.AUDIT: JobBitmask.AUDIT,
            JobSteps.PUBLISH: JobBitmask.PUBLISH,
            JobSteps.COMPLETE: JobBitmask.COMPLETE,
        }
        return mapping[self]

    @classmethod
    def next_step(cls, current_label: str) -> "JobSteps | None":
        """Finds the next step in the sequence based on a string label."""
        current_enum = cls[current_label.upper()]
        try:
            return cls(current_enum.value + 1)
        except ValueError:
            return None  # We have reached the end of the pipeline

    @classmethod
    def prev_step(cls, current_label: str) -> "JobSteps| None":
        """Finds the next step in the sequence based on a string label."""
        current_enum = cls[current_label.upper()]
        try:
            return cls(current_enum.value - 1)
        except ValueError:
            return None  # We have reached the start of the pipeline

    @classmethod
    def first_step(cls) -> "JobSteps":
        return cls(0)

    @classmethod
    def last_step(cls) -> "JobSteps":
        return cls(len(cls) - 1)


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

        raise ValueError(f"Invalid offset: {offset}")

    @abstractmethod
    def execute(self, job: "Job") -> str:
        """Execute the current JobStage with the given engine and dataframe.

        :param engine: The IngestionEngine instance.
        :type engine: IngestionEngine
        :param df: The dataframe to process in this stage.
        :type df: Any
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
        base_dir = Path(job.exec_ctx.workspace_dir)

        # Target: base/HOLD/job_id/run_id
        new_path = base_dir / category / f"{job.id}_{job.run_id}"

        # Ensure parent structure exists
        new_path.parent.mkdir(parents=True, exist_ok=True)

        if job.folder.exists():
            LOG.info("Moving metadata folder", src=job.folder, dst=new_path)
            # shutil.move handles cross-filesystem moves if necessary
            shutil.move(str(job.folder), str(new_path))

            # Update the job instance reference so subsequent saves hit the new path
            job._folder = new_path

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
        target = job.context.to_step

        # 2. MUTATE (same as before)
        if exception:
            # Create the error payload
            error_payload = ErrorPayload(
                step=self.name,
                error_type=type(exception).__name__,
                message=str(exception),
                stack_trace=traceback.format_exc(),
                # worker_id=job.worker_id,
                timestamp=time.time(),
            )
            error = msgspec.to_builtins(error_payload)

            # 2. ROUTING LOGIC (The "Sorting Hat")
            if isinstance(exception, (CircuitBreakerTripped, ClientCantConnect)):
                # If we fail during CompleteStep, it's Deferred (Ready to wrap up)
                # Otherwise, it's Blocked (Needs to re-run current step)
                # data["job_status"] = (
                #     JobStatus.DEFERRED if self.name == "CompleteStep"
                #     else JobStatus.BLOCKED
                # )

                HoldState(job).on_enter(data=error)
                target_category = "HOLD"
            else:
                FailedState(job).on_enter(data=error)
                target_category = "FAILED"

            self.move_to_folder(job, target_category)
        else:
            # Update the bitmask
            current_mask = data["bitmask"]
            new_mask = current_mask | self.bitmask

            next_step = JobSteps.next_step(self.name)
            reached_target = job.context.target_step == self.name

            if new_mask.is_fully_complete() or reached_target:
                SuccessState(job).on_enter(
                    data={
                        "bitmask": new_mask,
                        self.name: results or {},
                    }
                )
                LOG.info(
                    "Job reached target state", job_id=job.id, target=job.target_step
                )
            else:
                # Continue the chain (The Orchestrator will pick this up in the next scan)
                LOG.info(
                    "Job progressing to next step", job_id=job.id, next=next_step.label
                )

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
        for step_class in cls.__subclasses__():
            # If you have nested subclasses, you may want a recursive walk here.
            if getattr(step_class, "name", None) == name:
                idx = _JOB_ORDER.index(name)
                step = JobSteps(idx)
                return step_class(step=step)
        raise ValueError(f"Unknown step name: {name}")
