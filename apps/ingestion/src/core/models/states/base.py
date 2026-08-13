"""Base classes and protocols for task state management."""

from typing import TYPE_CHECKING, Any, ClassVar, Protocol, runtime_checkable

from src.core.models.task import ExecutionStatus, TaskSignal

if TYPE_CHECKING:
    from src.core.models.task import Task
    from src.core.orchestrator.enums import TaskRecord


@runtime_checkable
class TaskOutcome(Protocol):
    """Defines the outcome of a task execution."""

    folder: ClassVar[str | None]  # Where to move the task folder
    status: ClassVar[ExecutionStatus]  # Status to set in manifest
    is_final: ClassVar[bool]  # True if this is a terminal state
    signal: ClassVar[TaskSignal | None]  # Signal to send to orchestrator

    def matches(self, task: "Task", error: Exception | None = None) -> bool:
        """Check if this outcome applies to the current task state."""
        ...


@runtime_checkable
class DetectedState(Protocol):
    """Outcome determined by background monitoring (zombie, expiry)."""

    status: ClassVar[ExecutionStatus]

    def matches(self, record: "TaskRecord | None" = None, **kwargs: Any) -> bool: ...
