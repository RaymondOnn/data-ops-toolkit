from typing import TYPE_CHECKING, Any, ClassVar, Optional, Protocol, runtime_checkable

from apps.ingestion.src.core.models.task import ExecutionStatus, TaskSignal

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task
    from apps.ingestion.src.core.orchestrator.enums import JobRecord


@runtime_checkable
class ResultState(Protocol):
    """Base class for states that represent the outcome of a task's execution."""

    # Where to move the task folder (e.g., "FAILED", "RETRY", "active")
    folder_name: ClassVar[str | None]

    # The status to set in the manifest
    target_status: ClassVar[ExecutionStatus]

    # True if this state is an end-state (no further processing)
    is_terminal: ClassVar[bool]

    # The signal to drop for the orchestrator
    signal: ClassVar[TaskSignal | None]

    def is_applicable(self, task: "Task", exception: Exception | None = None) -> bool:
        """
        Determines if this state policy is applicable given the current task
        and exception.
        """
        ...


@runtime_checkable
class InferredState(Protocol):
    """Experts in orchestrator-side diagnoses (Zombie, Expired)."""

    target_status: ClassVar[ExecutionStatus]

    def is_applicable(
        self,
        record: Optional["JobRecord"] = None,
        **kwargs: Any,
    ) -> bool: ...
