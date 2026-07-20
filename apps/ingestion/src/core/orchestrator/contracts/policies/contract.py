from typing import Any, Protocol, runtime_checkable

import ray
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.orchestrator.common.task.cache import TaskCache
from apps.ingestion.src.core.orchestrator.common.task.compute import Compute
from apps.ingestion.src.core.orchestrator.enums import TaskMetadata
from loguru import logger

LOG = logger


@runtime_checkable
class AdmissionPolicy(Protocol):
    """Determines if a task can enter the execution cache."""

    def admit(
        self,
        cache: TaskCache,
        lock: Any,
        task_meta: TaskMetadata,
    ) -> bool:
        """Return True if task can be admitted, False otherwise."""
        ...


@runtime_checkable
class MaintenancePolicy(Protocol):
    """Handles background system health and task recovery."""

    def run(
        self,
        cache: TaskCache,
        lock: Any,
        active_tasks: dict[str, ray.ObjectRef],
        compute: Compute,
        exec_ctx: ExecutionContext,
    ) -> list[tuple[TaskMetadata, str]] | None:
        """Run one maintenance cycle."""
        ...
