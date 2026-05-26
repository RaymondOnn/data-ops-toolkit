import time
from typing import TYPE_CHECKING

from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import StageName
from apps.ingestion.src.core.models.task import ExecutionStatus
from apps.ingestion.src.services.registry import ServiceRegistry
from apps.ingestion.src.utils.constants import (
    CACHE_TASK_NAMESPACE,
    STRIP_TZ_FOR_DB,
)
from apps.ingestion.src.utils.dates import get_end_of_day_ts
from filelock import FileLock
from libs.cache.utils import get_cache
from libs.utils.dates import get_current_timestamp
from loguru import logger

from ..contracts.policies import AdmissionPolicy, MaintenancePolicy
from ..enums import JobUpdate, TaskMetadata, TaskRef
from .compute import Compute
from .state import StateStore

if TYPE_CHECKING:
    import ray

LOG = logger
STAGES_PRIORITY: dict[StageName, int] = {
    StageName.ARCHIVE: 100,
    StageName.PUBLISH: 80,
    StageName.WRITE: 60,
    StageName.TRANSFORM: 40,
    StageName.EXTRACT: 20,
    StageName.START: 10,
}


class TaskManager:
    def __init__(
        self,
        exec_ctx: ExecutionContext,
        state_store: StateStore,
        admission_policy: AdmissionPolicy,
        maintenance_policy: MaintenancePolicy,
        cache_dir: str = ".cache/ingestion",
    ):
        self.exec_ctx = exec_ctx
        self.state_store = state_store
        self.admission_policy = admission_policy
        self.maintenance_policy = maintenance_policy

        # Decentralized: TaskManager owns the active and data zones
        self.exec_ctx.active_path.mkdir(parents=True, exist_ok=True)
        self.exec_ctx.data_path.mkdir(parents=True, exist_ok=True)

        # 2. Initialize the Global Registry (Diskcache)
        # This ensures the shared cache path exists for all Ray workers
        ServiceRegistry.configure(
            self.exec_ctx.workspace_dir, self.exec_ctx.cache_config
        )
        self.registry = ServiceRegistry()

        # CRITICAL: Create cache directory in the parent process BEFORE
        # spinning up Ray workers to prevent SQLite race conditions for diskcache.
        if self.exec_ctx.cache_config.get("type") == "diskcache":
            cache_filepath = self.exec_ctx.cache_config.get("filepath", ".cache")
            (self.exec_ctx.workspace_dir / cache_filepath).mkdir(
                parents=True, exist_ok=True
            )

        self.cache = get_cache(self.exec_ctx.workspace_dir, self.exec_ctx.cache_config)
        self._loop_counter = 0
        self.lock = FileLock(self.exec_ctx.lock_file)
        self.is_degraded = False
        self._compute: Compute | None = None

        # Track state to prevent log spamming on every tick
        self._last_summary_state: tuple[int, bool] = (0, False)

        try:
            self.exec_ctx.check_serializability()
        except Exception as e:
            # If the context is not serializable, we cannot proceed with Ray workers.
            # Log the error and raise an exception to prevent silent failures.
            LOG.error(
                "Failed to initialize Ray workers due to unserializable context",
                error=str(e),
            )
            raise TypeError(f"ExecutionContext is not serializable: {e}") from e
        # Local tracker for active Ray tasks (ObjectRef -> task_key)
        self._active_tasks: dict[ray.ObjectRef, str] = {}

    @property
    def active_tasks(self) -> dict:
        """Expose active tasks as a read-only view or direct reference for the Janitor."""
        return self._active_tasks

    @property
    def compute(self) -> Compute:
        """Lazy-loaded compute coordinator."""
        if self._compute is None:
            self._compute = Compute(self.exec_ctx)
        return self._compute

    def queue_tasks(self, task_ref: TaskRef, config_file_path: str) -> TaskRef | None:
        if task_ref.status != ExecutionStatus.WAITING.value:
            task_ref = task_ref.with_updates(status=ExecutionStatus.WAITING.value)
        is_snapshot = "snapshot" in task_ref.job_id.lower()
        meta = TaskMetadata.from_ref(
            task_ref,
            config_file_path,
            expires_at=get_end_of_day_ts() if is_snapshot else None,
        )
        if self.admission_policy.validate_and_queue(
            self.cache, self.lock, task_ref, meta
        ):
            LOG.info("Queued Task", run_id=task_ref.run_id, stage=task_ref.stage)
            return task_ref
        return None

    def _process_tasks(self) -> None:
        # Delegate resource cleanup to maintenance policy logic as well
        self.maintenance_policy._cleanup_finished_tasks(
            self._active_tasks, self.compute
        )

        if self.exec_ctx.stop_at_ts is not None:
            return
        self.compute.refresh_resources()

        valid_statuses = {
            ExecutionStatus.WAITING.value,
            ExecutionStatus.RETRY.value,
            ExecutionStatus.BLOCKED.value,
        }
        scored_keys = []
        for status in valid_statuses:
            for k in self.cache.iterkeys(pattern=f"{CACHE_TASK_NAMESPACE}:{status}:*"):
                try:
                    ref = TaskRef.from_str(k)
                    priority = STAGES_PRIORITY.get(StageName.from_label(ref.stage), 0)
                    scored_keys.append((priority, k))
                except ValueError:
                    continue

        candidate_keys = [k for _, k in sorted(scored_keys, reverse=True)]
        dispatch_count = 0

        for key in candidate_keys:
            task_ref = TaskRef.from_str(key)
            task_meta: TaskMetadata = self.cache.get(key)
            if not task_meta:
                continue

            ready = (
                (task_ref.status == ExecutionStatus.WAITING.value)
                or (
                    task_ref.status == ExecutionStatus.RETRY.value
                    and time.time() >= task_meta.last_hb
                )
                or (
                    task_ref.status == ExecutionStatus.BLOCKED.value
                    and ServiceRegistry.is_healthy(task_meta.blocked_by or "")
                    and time.time() >= task_meta.last_hb
                )
            )

            if not ready:
                continue
            task_meta.status, task_meta.last_hb = (
                ExecutionStatus.DISPATCHED.value,
                time.time(),
            )
            new_key = task_ref.build(status=task_meta.status)
            ref = self.compute.spawn_worker(
                stage=StageName.from_label(task_ref.stage), key=new_key
            )

            if ref:
                self.state_store.update_run(
                    task_ref.run_id,
                    JobUpdate(
                        JOB_ID=task_meta.job_id,
                        DATASET_ID=task_meta.dataset_id,
                        PARTITION_DATE=task_meta.partition_date,
                        JOB_STATUS=task_meta.status,
                        CURRENT_STAGE=task_ref.stage,
                        LAST_UPDATED_AT_TS_LC=get_current_timestamp(
                            strip_tz=STRIP_TZ_FOR_DB
                        ).isoformat(sep=" "),
                    ),
                )
                with self.lock:
                    self.cache.pop(key, None)
                    self.cache[new_key] = task_meta
                self._active_tasks[ref] = new_key
                dispatch_count += 1
                LOG.success(
                    "Dispatching task",
                    job_id=task_meta.job_id,
                    run_id=task_meta.run_id,
                    stage=task_ref.stage,
                )

    def _get_all_keys(self) -> list[str]:
        """
        Fetches a snapshot of all task keys from the cache.
        """
        return list(self.cache.iterkeys(pattern=f"{CACHE_TASK_NAMESPACE}:*"))

    def recover_zombie_tasks(self) -> None:
        self.maintenance_policy.run(
            self.cache, self.lock, self._active_tasks, self.compute, self.exec_ctx
        )
