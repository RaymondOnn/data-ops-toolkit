from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from apps.ingestion.src.core.models.task import ExecutionStatus, TaskSignal

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task
    from apps.ingestion.src.core.orchestrator.enums import JobRecord


class LifecycleState(ABC):
    pass

class ResultState(LifecycleState):
    """Base class for states that represent the outcome of a task's execution."""

    folder_name: ClassVar[str | None] = None  # Where to move the task folder (e.g., "FAILED", "RETRY", "active")
    target_status: ClassVar[ExecutionStatus] = ExecutionStatus.UNKNOWN  # The status to set in the manifest
    is_terminal: ClassVar[bool] = False  # True if this state is an end-state (no further processing)
    signal: ClassVar[TaskSignal | None] = None  # The signal to drop for the orchestrator

    @classmethod
    @abstractmethod
    def is_applicable(cls, task: "Task", exception: Exception | None = None) -> bool:
        """
        Determines if this state policy is applicable given the current task 
        and exception.
        """
        pass

class InferredState(LifecycleState):
    """Experts in orchestrator-side diagnoses (Zombie, Expired)."""
    target_status: ClassVar[ExecutionStatus] = ExecutionStatus.UNKNOWN 

    @classmethod
    @abstractmethod
    def is_applicable(
        cls,
        record: Optional["JobRecord"] = None, **kwargs: Any,
    ) -> bool:
        pass
