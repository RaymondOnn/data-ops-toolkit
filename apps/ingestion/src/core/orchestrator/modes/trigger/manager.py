from typing import TYPE_CHECKING, Any

import ray
from apps.ingestion.src.core.models.task import TaskRef
from apps.ingestion.src.core.orchestrator.contracts.policies import (
    AdmissionPolicy,
    MaintenancePolicy,
)
from apps.ingestion.src.core.orchestrator.enums import TaskMetadata
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.contexts.execution import ExecutionContext
    from apps.ingestion.src.core.orchestrator.common.task.compute import Compute

LOG = logger


class OverwriteAdmission(AdmissionPolicy):
    """Admission policy that ensures a clean slate for manual/triggered runs."""

    def admit(
        self,
        cache: Any,
        lock: Any,
        task_ref: TaskRef,
        metadata: TaskMetadata,
    ) -> bool:
        """Evicts any existing run for the partition and admits the new task.

        Args:
            cache: The task metadata cache.
            lock: Reentrant lock for cache atomicity.
            task_ref: Reference for the new task.
            metadata: Metadata associated with the new task.

        Returns:
            bool: Always True, as it forcefully admits the task.

        Decision: Forced Re-run logic.
        In Trigger (manual) mode, a user request implies that any previous
        state for that partition (even if currently running or failed)
        should be disregarded in favor of the new command. By inlining
        the eviction pattern and logic, we ensure the "Clear-then-Set"
        sequence is atomic within the lock.
        """
        pattern = f"{CACHE_TASK_NAMESPACE}:*:*:{task_ref.task_key}:*"
        existing = next(iter(cache.iterkeys(pattern=pattern)), None)

        with lock:
            if existing:
                LOG.info(f"Evicting existing task for fresh run: {existing}")
                cache.pop(existing, None)
            cache[task_ref.build()] = metadata

        return True


class NoOpMaintenance(MaintenancePolicy):
    """Minimal maintenance policy for synchronous execution modes."""

    def run(
        self,
        cache: Any,
        lock: Any,
        active_refs: dict[ray.ObjectRef, str],
        compute: "Compute",
        exec_ctx: "ExecutionContext",
    ) -> list[tuple[TaskMetadata, str]] | None:
        """Performs basic resource reclamation without active self-healing.

        Args:
            cache: The task metadata cache.
            lock: Reentrant lock for cache operations.
            active_refs: Mapping of Ray ObjectRefs to cache keys.
            compute: The compute resource manager.
            exec_ctx: Global application settings.

        Decision: Foreground Ownership.
        In Trigger mode, the foreground loop manages task transitions and
        timeouts. The background maintenance thread is restricted to
        cleaning up Ray handles (`active_refs`) to prevent resource
        exhaustion, while leaving state transitions to the primary
        execution thread.
        """
        self.cleanup_tasks(active_refs, compute)
        return None

    def cleanup_tasks(
        self,
        active_refs: dict[ray.ObjectRef, str],
        compute: "Compute",
    ) -> None:
        """Identifies finished Ray tasks and releases compute slots.

        Args:
            active_refs: Mapping of active Ray references.
            compute: Resource coordinator.

        Decision: Resource Integrity.
        In Trigger mode, cleanup is the only background responsibility.
        This ensures that even if the manual run process stays open, it
        doesn't leak Ray worker handles.
        """
        if not active_refs:
            return

        ready_refs, _ = ray.wait(list(active_refs.keys()), timeout=0)

        for ref in ready_refs:
            task_key = active_refs.pop(ref, None)
            compute.reclaim_resources(ref)
            LOG.debug(f"Cleaned up finished task: {task_key}")
