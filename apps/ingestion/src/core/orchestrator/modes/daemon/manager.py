import time
from typing import Any

import ray
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import Stage
from apps.ingestion.src.core.models.states import ZombieState
from apps.ingestion.src.core.models.task import (
    ExecutionStatus,
    Task,
    TaskRef,
    TaskSignal,
)
from apps.ingestion.src.core.orchestrator.common.task.compute import Compute
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
        cache: Any,
        lock: Any,
        task_ref: TaskRef,
        metadata: TaskMetadata,
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
        pattern = f"{CACHE_TASK_NAMESPACE}:*:*:{task_ref.task_key}:*"

        if any(cache.iterkeys(pattern=pattern)):
            LOG.warning(
                f"Rejecting duplicate task for partition "
                f"{task_ref.task_key} - already active"
            )
            return False

        with lock:
            cache.set(key=task_ref.build(), value=metadata)

        return True


class ProactiveMaintenance(MaintenancePolicy):
    """Maintenance policy for the Always-On mode that detects and recovers
    stalled tasks.
    """

    def run(
        self,
        cache: Any,
        lock: Any,
        active_refs: dict[ray.ObjectRef, str],
        compute: Compute,
        exec_ctx: ExecutionContext,
    ) -> list[tuple[TaskMetadata, str]] | None:
        """Executes one full maintenance cycle.

        Args:
            cache: The task metadata cache.
            lock: Atomic lock for cache operations.
            active_refs: Map of Ray references to cache keys.
            compute: The compute resource manager.
            exec_ctx: Global execution settings.

        Decision: Multi-Phase Reconciliation.
        The maintenance loop is split into cleanup, resource reconciliation,
        and zombie recovery phases. This ensures that the engine's internal
        view of cluster state is always synchronized with the physical
        Ray GCS and filesystem markers.
        """
        # Phase 1: Clean up finished tasks
        self.cleanup_tasks(active_refs, compute)

        # Phase 2: Reconcile compute counts
        compute.reconcile_counts()

        # Phase 3: Find and recover zombie tasks
        return self._recover_zombie_tasks(cache, lock, active_refs, compute, exec_ctx)

    def cleanup_tasks(
        self,
        active_refs: dict[ray.ObjectRef, str],
        compute: Compute,
    ) -> None:
        """Reclaims resources from completed Ray tasks.

        Args:
            active_refs: Mapping of Ray ObjectRefs to internal cache keys.
            compute: The compute resource manager.

        Decision: Non-Blocking Cleanup.
        We use ray.wait with a timeout of 0 to identify completed tasks
        without blocking the main thread, ensuring system responsiveness
        even during heavy job loads. This is called both during scheduled
        maintenance and on every dispatch tick.
        """
        if not active_refs:
            return

        ready_refs, _ = ray.wait(list(active_refs.keys()), timeout=0)

        for ref in ready_refs:
            task_key = active_refs.pop(ref, None)
            compute.reclaim_resources(ref)

            # Check if the Ray task failed
            try:
                ray.get(ref)
            except Exception as e:
                LOG.error(f"Ray worker for {task_key} failed with exception: {e}")

            if task_key:
                LOG.debug(f"Cleaned up finished task: {task_key}")

    def _recover_zombie_tasks(
        self,
        cache: Any,
        lock: Any,
        active_refs: dict[ray.ObjectRef, str],
        compute: Compute,
        exec_ctx: ExecutionContext,
    ) -> list[tuple[TaskMetadata, str]]:
        """Identifies and resurrects stalled (zombie) tasks from the hot cache.

        Decision: Integrated Maintenance.
        By inlining detection and recovery into a single pass, we reduce
        method-call overhead and simplify the logical flow for identifying
        'ghost' tasks that have lost their physical Ray connection.
        """
        dispatched_statuses = {s.value for s in ExecutionStatus.dispatched_statuses()}
        pattern = f"{CACHE_TASK_NAMESPACE}:*:*"
        recovered_tasks = []

        for cache_key in cache.iterkeys(pattern=pattern):
            task_ref = TaskRef.from_key(cache_key)

            # 1. Filter: Only check tasks in dispatched (non-terminal) states
            if task_ref.status not in dispatched_statuses:
                continue

            metadata = cache.get(cache_key)
            if not metadata:
                continue

            # 2. Check: Determine if the task is a zombie using ZombieState logic
            is_zombie = ZombieState.matches(
                cache_key=cache_key,
                meta=metadata,
                active_tasks=active_refs,
                exec_ctx=exec_ctx,
            )

            if not is_zombie:
                continue

            # 3. Action: Perform recovery or failure transition
            LOG.warning(f"Zombie task detected, attempting recovery: {task_ref.run_id}")

            if exec_ctx.disable_self_healing:
                self._mark_task_failed(task_ref, exec_ctx, cache, lock, cache_key)
                continue

            # Reclaim compute resources (cleanup active_refs)
            self._reclaim_resources(cache_key, active_refs, compute)

            # Perform physical and logical resurrection
            resurrected = self._resurrect_task(
                cache_key,
                task_ref,
                metadata,
                cache,
                lock,
                active_refs,
                compute,
                exec_ctx,
            )
            if resurrected:
                recovered_tasks.append(resurrected)

        return recovered_tasks

    def _mark_task_failed(
        self,
        task_ref: TaskRef,
        exec_ctx: ExecutionContext,
        cache: Any,
        lock: Any,
        cache_key: str,
    ) -> None:
        """Permanently marks a zombie task as failed when self-healing is disabled.

        Decision: Terminal Transition.
        When self-healing is globally disabled, we opt for safe termination.
        The task is moved to the FAILED vault, allowing operators to
        inspect the state manually via the 'resume' command after fixing
        the root cause.
        """
        task = Task(
            task_ref=task_ref, worker_id="maintenance-recovery", exec_ctx=exec_ctx
        )
        task.update_manifest(
            {"status": ExecutionStatus.FAILED.value, "remarks": "Zombie"}
        )
        task.send_signal(TaskSignal.FAIL)
        task.move_to("FAILED")

        with lock:
            cache.pop(cache_key, None)

    def _reclaim_resources(
        self,
        cache_key: str,
        active_refs: dict[ray.ObjectRef, str],
        compute: Compute,
    ) -> None:
        """Forcefully releases compute resources associated with a zombie task.

        Decision: Orphan Cleanup.
        Zombies by definition have no active physical worker, but may still
        hold logical slots in the Compute manager. We explicitly reclaim
        these to prevent resource leakage.
        """
        for ref, active_key in list(active_refs.items()):
            if active_key == cache_key:
                compute.reclaim_resources(ref)
                active_refs.pop(ref)
                break

    def _resurrect_task(
        self,
        cache_key: str,
        task_ref: TaskRef,
        metadata: TaskMetadata,
        cache: Any,
        lock: Any,
        active_refs: dict[ray.ObjectRef, str],
        compute: Compute,
        exec_ctx: ExecutionContext,
    ) -> tuple[TaskMetadata, str] | None:
        """Updates task state to trigger a re-execution of a stalled run.

        Decision: Clean Slate Resume.
        Resurrection unsets blocking markers and reverts the status to
        WAITING, allowing the task manager to treat it as a fresh dispatch
        while preserving historical stage progress in the manifest.
        """
        LOG.info(f"Resurrecting zombie task: {task_ref.run_id}")

        task = Task(
            task_ref=task_ref, worker_id="maintenance-recovery", exec_ctx=exec_ctx
        )

        # 1. Signal Flush: Purge filesystem markers (Inlined)
        (task.workspace.path / ".retrying").unlink(missing_ok=True)
        (task.workspace.path / ".blocked").unlink(missing_ok=True)

        # 2. Determine resume point (Inlined)
        resume_stage = task.context.from_stage or Stage.first().value

        # 3. Update manifest for fresh start
        task.update_manifest(
            {
                "status": ExecutionStatus.WAITING.value,
                "current_stage": resume_stage,
                "remarks": "Maintenance: Recovered from zombie state.",
            }
        )

        # 4. Atomic Cache Key Rotation (Inlined)
        with lock:
            # Remove old entry
            cache.pop(cache_key, None)

            # Update metadata with new state
            metadata.status = ExecutionStatus.WAITING.value
            metadata.current_stage = resume_stage
            metadata.last_hb = time.time()

            # Create new cache key with updated state
            new_cache_key = task_ref.with_updates(
                status=ExecutionStatus(metadata.status), stage=resume_stage
            ).build()

            cache[new_cache_key] = metadata

        # 5. Signal engine to re-evaluate
        task.send_signal(TaskSignal.SYNC)

        return metadata, resume_stage
