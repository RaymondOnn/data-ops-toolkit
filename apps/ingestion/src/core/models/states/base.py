from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task
    from apps.ingestion.src.core.orchestrator.enums import JobRecord


class LifecycleState(ABC):
    folder_name: str  # e.g., "HOLD", "FAILED", "DONE"

    @abstractmethod
    def on_enter(self, task: "Task", data: dict[str, Any] | None = None) -> None:
        """Logic executed when a task is moved into this state."""
        pass

    @abstractmethod
    def can_recover(self, task: "Task", **kwargs: Any) -> bool:
        """Logic to determine if the task can return to 'active'."""
        pass


class ResultState(LifecycleState):
    """Experts in assessing worker-reported results (Success, Fail, Retry)."""

    @classmethod
    @abstractmethod
    def is_applicable(
        cls, task: "Task", exception: Exception | None = None
    ) -> bool:
        """Determines if the stage execution resulted in this outcome."""
        pass


class InferredState(LifecycleState):
    """Experts in orchestrator-side diagnoses (Zombie, Expired)."""

    @classmethod
    @abstractmethod
    def is_applicable(cls, record: Optional["JobRecord"] = None, **kwargs: Any) -> bool:
        """Determines if the system state (TTL, heartbeats) matches this diagnosis."""
        pass
