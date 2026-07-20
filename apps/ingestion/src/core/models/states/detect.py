"""Task outcome policies for success, failure, retry, and background detection."""

import time
from typing import TYPE_CHECKING, Any

import ray
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.common import find_path
from apps.ingestion.src.utils.constants import MANIFEST_FILENAME, STRIP_TZ_FOR_DB
from libs.utils.dates import current_timestamp, parse_timestamp
from loguru import logger

from .base import DetectedState

if TYPE_CHECKING:
    from apps.ingestion.src.core.orchestrator.enums import TaskRecord

LOG = logger


class ZombieState(DetectedState):
    """Task is a zombie (silently died)."""

    status = ExecutionStatus.WAITING

    @classmethod
    def matches(cls, record: "TaskRecord | None" = None, **kwargs: Any) -> bool:
        """Check if task is a zombie using triple verification."""
        metadata = kwargs.get("metadata")
        active_tasks = kwargs.get("active_tasks", {})
        exec_ctx = kwargs.get("exec_ctx")

        if not metadata or not exec_ctx:
            return False

        # 1. Still active in Ray?
        if metadata.run_id in active_tasks.values():
            return cls._is_ray_task_stuck(
                metadata.run_id, active_tasks[metadata.run_id], exec_ctx
            )

        # 2. Heartbeat too old (>5 minutes)?
        if time.time() - metadata.last_hb < 300:
            return False

        # 3. Manifest file recently modified?
        run_path = find_path(exec_ctx.active_path, metadata.run_id)
        if run_path and cls._is_manifest_recent(run_path):
            return False

        LOG.warning(f"Zombie task detected: {metadata.run_id}")
        return True

    @classmethod
    def _is_ray_task_stuck(cls, run_id: str, ref: Any, exec_ctx: Any) -> bool:
        """Helper to verify if an active cluster task is physically stuck or crashed."""
        if exec_ctx.ray_mode == "LOCAL" or not ray.is_initialized():
            return False

        try:
            ready, _ = ray.wait([ref], timeout=0.0)
            if ready:
                LOG.error(
                    f"Physical Ray task for {run_id} finished or crashed, but state is stuck."
                )
                return True
        except Exception as e:
            LOG.warning(
                f"Ray task exception caught during reconciliation for {run_id}: {e}"
            )

        return False

    @staticmethod
    def _is_manifest_recent(run_path: Any) -> bool:
        """Helper to verify recent file modifications on the run manifest."""
        manifest = run_path / MANIFEST_FILENAME
        return manifest.exists() and time.time() - manifest.stat().st_mtime < 300


class ExpiredState(DetectedState):
    """Task has exceeded its TTL."""

    status = ExecutionStatus.EXPIRED

    @classmethod
    def matches(cls, record: "TaskRecord | None" = None, **kwargs: Any) -> bool:
        if not record or not record.IS_SNAPSHOT or not record.EXPIRATION_THRESHOLD:
            return False

        # Don't expire terminal tasks
        if ExecutionStatus(record.JOB_STATUS).is_terminal:
            return False

        # Don't expire if extraction already completed (sunk cost)
        if "EXT+" in str(getattr(record, "JOB_BITMASK", "")):
            return False

        threshold = parse_timestamp(record.EXPIRATION_THRESHOLD, naive=STRIP_TZ_FOR_DB)
        current = kwargs.get("now") or current_timestamp(naive=STRIP_TZ_FOR_DB)

        return current > threshold
