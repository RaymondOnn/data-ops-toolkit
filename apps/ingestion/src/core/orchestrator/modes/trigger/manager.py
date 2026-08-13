from typing import TYPE_CHECKING, Any

import ray
from loguru import logger

from src.core.orchestrator.common.task.cache import TaskCache
from src.core.orchestrator.contracts.policies import (
    AdmissionPolicy,
    MaintenancePolicy,
)
from src.core.orchestrator.enums import TaskMetadata

if TYPE_CHECKING:
    from src.core.contexts.execution import ExecutionContext

LOG = logger


class OverwriteAdmission(AdmissionPolicy):
    """Admission policy that ensures a clean slate for manual/triggered runs."""

    def admit(
        self,
        cache: TaskCache,
        lock: Any,
        task_meta: TaskMetadata,
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
        pattern = f"{cache.prefix}:*:{task_meta.run_id}"
        existing_keys = list(cache.client.iterkeys(pattern=pattern))

        with lock:
            for old_key in existing_keys:
                LOG.info(
                    f"Evicting conflicting state key for fresh forced trigger override: {old_key}"
                )
                cache.client.pop(old_key, None)

            # Admit the new task context clean
            cache_key = task_meta.generate_cache_key()
            cache.client.set(cache_key, task_meta)

        return True


class NoOpMaintenance(MaintenancePolicy):
    """Minimal maintenance policy for synchronous execution modes."""

    def run(
        self,
        cache: TaskCache,
        lock: Any,
        active_tasks: dict[str, ray.ObjectRef],
        exec_ctx: "ExecutionContext",
    ) -> list[TaskMetadata] | None:
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
