from typing import Any

import ray
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import Stage
from apps.ingestion.src.core.models.states import ZombieState
from apps.ingestion.src.core.models.task import (
    ExecutionStatus,
)
from apps.ingestion.src.core.orchestrator.common.task.cache import TaskCache
from apps.ingestion.src.core.orchestrator.contracts.policies import (
    AdmissionPolicy,
    MaintenancePolicy,
)
from apps.ingestion.src.core.orchestrator.enums import TaskMetadata
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE
from loguru import logger

LOG = logger


class DenyDuplicateAdmission(AdmissionPolicy):
    """Admission policy that prevents overlapping runs for the same partition."""

    def admit(
        self,
        cache: TaskCache,
        lock: Any,
        task_meta: TaskMetadata,
    ) -> bool:
        """Validates and admits a task if no active run for the partition exists.

        Args:
            cache: The task metadata cache backend.
            lock: Reentrant lock for cache atomicity.
            task_ref: Reference for the new task containing identity.
            metadata: Metadata associated with the new task.

        Returns:
            bool: True if the task was admitted, False if rejected.

        Decision: Partition Isolation.
        By checking for existing keys matching the task identifier before
        admission, we prevent race conditions where multiple instances of
        the same job/partition might try to execute simultaneously.
        Inlining the helper logic ensures the 'Check-then-Set' operation
        is clear and remains atomic under the provided lock.
        """
        # Query across active lifecycle milestones (WAITING, DISPATCHED, RUNNING, BLOCKED, RETRY)
        active_statuses = [
            ExecutionStatus.WAITING.value,
            ExecutionStatus.DISPATCHED.value,
            ExecutionStatus.RUNNING.value,
            ExecutionStatus.BLOCKED.value,
            ExecutionStatus.RETRY.value,
        ]

        # Check if this specific partition identity is already present in any active state
        for status in active_statuses:
            # Build search pattern using the domain prefix configuration layout
            pattern = f"{CACHE_TASK_NAMESPACE}:{task_meta.current_stage}:{status}:{task_meta.run_id}"
            if cache.client.exists(pattern):
                LOG.warning(
                    f"Admission denied. Duplicate active task found for run: {task_meta.run_id}"
                )
                return False

        with lock:
            # Safe entry allocation using the modern structure
            target_key = task_meta.generate_cache_key()
            cache.client.set(target_key, task_meta)

        return True


class ProactiveMaintenance(MaintenancePolicy):
    """Maintenance policy for the Always-On mode that detects and recovers
    stalled tasks.
    """

    def run(
        self,
        cache: TaskCache,  # TaskCache instance
        lock: Any,  # Threading lock
        active_tasks: dict[
            str, ray.ObjectRef
        ],  # TaskManager._dispatched_tasks reference
        exec_ctx: ExecutionContext,
    ) -> list[TaskMetadata] | None:
        """Runs a self-healing iteration over active cache allocations.

        Identifies stuck or orphaned DISPATCHED/RUNNING states and reconciles them
        against live cluster futures to clear zombie pipelines.
        """
        # 1. Scan the hot cache for workloads currently in progress
        active_patterns = [
            f"{CACHE_TASK_NAMESPACE}:*:{ExecutionStatus.DISPATCHED.value}:*",
            f"{CACHE_TASK_NAMESPACE}:*:{ExecutionStatus.RUNNING.value}:*",
        ]

        keys_to_evaluate = []
        for pattern in active_patterns:
            keys_to_evaluate.extend(list(cache.client.iterkeys(pattern=pattern)))

        if not keys_to_evaluate:
            return None

        recovered_tasks = []

        # 2. Reconcile cache keys against physical cluster handles
        for cache_key in keys_to_evaluate:
            metadata = cache.get(cache_key)
            if not metadata:
                continue

            # Delegate to policy rule checking, providing the active Ray future map
            is_zombie = ZombieState.matches(
                metadata=metadata,
                active_tasks=active_tasks,
                exec_ctx=exec_ctx,
                cache_key=cache_key,
            )

            if is_zombie:
                LOG.warning(f"Recovery triggered for zombie task: {metadata.run_id}")

                with lock:
                    # Clean up the manager's tracking table for the dead future handle
                    active_tasks.pop(metadata.run_id, None)

                    # Determine where this task should resume
                    resume_stage = metadata.current_stage or Stage.first().value

                    # Update metadata context fields safely
                    metadata.status = ExecutionStatus.WAITING.value
                    metadata.current_stage = resume_stage

                    # Leverage TaskCache.transition_state to rotate keys cleanly
                    # This removes the old DISPATCHED/RUNNING key and writes a clean WAITING record
                    cache.transition_state(
                        metadata=metadata,
                        next_status=ExecutionStatus.WAITING,
                        next_stage=Stage(resume_stage),
                    )

                # Return the recovered task so the daemon engine knows to queue it
                recovered_tasks.append(metadata)

        return recovered_tasks if recovered_tasks else None
