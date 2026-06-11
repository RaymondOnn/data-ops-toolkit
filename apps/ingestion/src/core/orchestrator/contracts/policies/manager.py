from typing import Any, Protocol, runtime_checkable

import ray
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.models.task import (
    TaskRef,
)
from apps.ingestion.src.core.orchestrator.common.compute import Compute
from apps.ingestion.src.core.orchestrator.enums import TaskMetadata
from loguru import logger

LOG = logger


@runtime_checkable
class AdmissionPolicy(Protocol):
    """Determines if a task can enter the execution cache."""

    def admit(
        self,
        cache: Any,
        lock: Any,
        task_ref: TaskRef,
        metadata: TaskMetadata,
    ) -> bool:
        """Return True if task can be admitted, False otherwise."""
        ...


@runtime_checkable
class MaintenancePolicy(Protocol):
    """Handles background system health and task recovery."""

    def run(
        self,
        cache: Any,
        lock: Any,
        active_refs: dict[ray.ObjectRef, str],
        compute: Compute,
        exec_ctx: ExecutionContext,
    ) -> None:
        """Run one maintenance cycle."""
        ...

    def cleanup_tasks(
        self,
        active_refs: dict[ray.ObjectRef, str],
        compute: Compute,
    ) -> None:
        """Reclaims resources from completed Ray tasks."""
        ...
