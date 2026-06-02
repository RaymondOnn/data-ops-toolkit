from typing import Any

from apps.ingestion.src.core.models.task import TaskRef
from apps.ingestion.src.core.orchestrator.contracts.policies import (
    AdmissionPolicy,
    MaintenancePolicy,
)
from apps.ingestion.src.core.orchestrator.enums import TaskMetadata
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE
from loguru import logger

LOG = logger


class ForceAdmission(AdmissionPolicy):
    """Trigger Mode: Evict existing partition state to ensure a clean manual run."""

    def validate_and_queue(
        self, cache: Any, lock: Any, task_ref: TaskRef, meta: TaskMetadata
    ) -> bool:
        pattern = f"{CACHE_TASK_NAMESPACE}:*:*:{task_ref.identifier}:*"
        existing = next(iter(cache.iterkeys(pattern=pattern)), None)

        if existing:
            LOG.info(
                "Trigger mode: Evicting existing task for fresh run.",
                identifier=task_ref.identifier,
            )
            with lock:
                cache.pop(existing, None)

        with lock:
            cache[task_ref.build()] = meta
        return True


class NullMaintenance(MaintenancePolicy):
    """Sync Mode: Maintenance is unnecessary or handled by the foreground loop."""

    def run(self, cache, lock, active_tasks, compute, exec_ctx) -> None:
        self._cleanup_finished_tasks(active_tasks, compute)
