from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task


class LifecycleState(ABC):
    folder_name: str  # e.g., "HOLD", "FAILED", "DONE"

    def __init__(self, task: "Task"):
        self.task = task

    @classmethod
    @abstractmethod
    def is_applicable(cls, task: "Task", exception: Exception | None = None) -> bool:
        """Logic to determine if the task outcome matches this state."""
        pass
    
    @abstractmethod
    def on_enter(self, data: dict[str, Any]) -> None:
        """Logic executed when a task is moved into this state."""
        pass

    @abstractmethod
    def can_recover(self) -> bool:
        """Logic to determine if the task can return to 'active'."""
        pass
