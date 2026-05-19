import time
from typing import Any

from apps.ingestion.src.core.models.states import ZombieState
from apps.ingestion.src.core.models.task import (
    ExecutionStatus,
    Task,
    TaskRef,
    TaskSignal,
)
from apps.ingestion.src.core.orchestrator.enums import TaskMetadata
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE
from loguru import logger

from ...contracts.policies import AdmissionPolicy, MaintenancePolicy

LOG = logger


class StrictAdmission(AdmissionPolicy):
    """Reactive Mode: Prevent overlapping runs for the same partition."""

    def validate_and_queue(
        self, cache: Any, lock: Any, task_ref: TaskRef, meta: TaskMetadata
    ) -> bool:
        pattern = f"{CACHE_TASK_NAMESPACE}:*:*:{task_ref.identifier}:*"
        if any(cache.iterkeys(pattern=pattern)):
            LOG.warning(
                "Partition already has an active run. Skipping.",
                identifier=task_ref.identifier,
            )
            return False

        with lock:
            cache[task_ref.build()] = meta
        return True



class ReactiveMaintenance(MaintenancePolicy):
    """Always-On: Performs proactive resource reclamation and zombie detection."""

    def run(self, cache, lock, active_tasks, compute, exec_ctx) -> None:
        self._cleanup_finished_tasks(active_tasks, compute)
        compute.reconcile_counts()

        dispatched_statuses = {s.value for s in ExecutionStatus.dispatched_statuses()}
        for key in cache.iterkeys(pattern=f"{CACHE_TASK_NAMESPACE}:*:*"):
            task_ref = TaskRef.from_str(key)
            if task_ref.status not in dispatched_statuses:
                continue

            meta = cache.get(key)
            if not meta or not ZombieState.is_applicable(
                record=key,
                meta=meta,
                active_tasks=active_tasks,
                exec_ctx=exec_ctx,
            ):
                continue

            task = Task(
                task_ref=task_ref, worker_id="engine-recovery", exec_ctx=exec_ctx
            )
            if exec_ctx.disable_self_healing:
                task.update_manifest(
                    {"status": ExecutionStatus.FAILED, "remarks": "Zombie"}
                )
                task.request_status_sync(TaskSignal.FAIL)
                task.move_to_folder("FAILED")
                with lock:
                    cache.pop(key, None)
                continue

            if task.workspace.exists():
                m_file = task.workspace.manifest_path
                if m_file.exists() and (time.time() - m_file.stat().st_mtime < 300):
                    meta.last_hb = time.time()
                    with lock:
                        cache[key] = meta
                    continue

                for ref, active_key in list(active_tasks.items()):
                    if active_key == key:
                        compute.reclaim_resources(ref)
                        active_tasks.pop(ref)

                LOG.warning("Zombie task detected", key=key)
                self._recover_task(key, cache, lock, active_tasks, compute, exec_ctx)



