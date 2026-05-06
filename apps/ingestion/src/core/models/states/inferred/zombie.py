import time
from typing import TYPE_CHECKING, Any, Optional

from apps.ingestion.src.core.models.states.base import InferredState
from apps.ingestion.src.core.models.task.enums import TaskSignal
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.common import find_path
from apps.ingestion.src.utils.constants import MANIFEST_FILENAME
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task
    from apps.ingestion.src.core.orchestrator.enums import JobRecord

LOG = logger


class ZombieState(InferredState):
    """
    The expert on detecting and recovering tasks that have died silently
    (e.g., SIGKILL, Ray node failure, OOM).
    """

    folder_name = "active"

    @classmethod
    def is_applicable(
        cls,
        record: Optional["JobRecord"] = None,
        **kwargs: Any,
    ) -> bool:
        """
        DIAGNOSIS: Performs the 'Triple Check' to identify a true zombie.
        """
        meta = kwargs.get("meta")
        active_ray_tasks = kwargs.get("active_ray_tasks", {})
        exec_ctx = kwargs.get("exec_ctx")
        cache_key = kwargs.get("cache_key")

        if not meta or not exec_ctx:
            return False

        # 1. RAY CHECK: If the Orchestrator still holds a handle to a Ray task,
        # the process is technically alive (even if it is slow).
        if cache_key in active_ray_tasks.values():
            return False

        # 2. LOGICAL HEARTBEAT CHECK: Is the cache timestamp older than 5 minutes?
        if time.time() - meta.last_hb < 300:
            return False

        # 3. PHYSICAL HEARTBEAT CHECK: Has the manifest file been touched recently?
        active_path = find_path(exec_ctx.active_path, meta.run_id)
        if active_path:
            manifest_file = active_path / MANIFEST_FILENAME
            if manifest_file.exists():
                mtime = manifest_file.stat().st_mtime
                if time.time() - mtime < 300:
                    return False

        return True

    def on_enter(self, task: "Task", data: dict[str, Any] | None = None) -> None:
        """
        Resurrects the task by resetting its manifest for the engine to pick up.
        """
        LOG.warning("ZombieState: resurrecting dead task", run_id=task.run_id)

        # 1. Reset manifest to WAITING
        task.update_manifest(
            {
                "status": ExecutionStatus.WAITING.value,
                "current_stage": None,  # Force re-initialization of the stage
                "remarks": "Recovered from silent death (Zombie)",
            }
        )

        # 2. Clean up indicators
        (task.folder / ".retrying").unlink(missing_ok=True)
        (task.folder / ".blocked").unlink(missing_ok=True)

        # 3. Signal change to database
        task.request_status_sync(TaskSignal.SYNC)

    def can_recover(self, task: "Task", **kwargs: Any) -> bool:
        return True
