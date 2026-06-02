import time
from typing import TYPE_CHECKING

from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import StageName
from apps.ingestion.src.core.models.task import ExecutionStatus
from apps.ingestion.src.core.orchestrator.contracts.policies import (
    AdmissionPolicy,
    MaintenancePolicy,
)
from apps.ingestion.src.core.orchestrator.enums import JobUpdate, TaskMetadata, TaskRef
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.services.registry import ServiceRegistry
from apps.ingestion.src.utils.constants import (
    CACHE_TASK_NAMESPACE,
    STRIP_TZ_FOR_DB,
)
from apps.ingestion.src.utils.dates import get_end_of_day_ts
from filelock import FileLock
from libs.cache.factory import get_cache
from libs.utils.dates import get_current_timestamp
from loguru import logger

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
    """Orchestration Control Plane for task queuing and dispatch.

    Decision: Centralized Resource Awareness.
    The TaskManager combines logical admission control (AdmissionPolicy),
    hardware resource management (Compute), and external health (Registry)
    to ensure tasks are only dispatched when the environment is ready.

    Decision: Zero-Touch Recovery.
    Blocked tasks are held in the 'active/' workspace to allow for seamless
    recovery once infrastructure outages are resolved.

    Decision: Active Probing Cooldown.
    Probes are expensive. We enforce a temporal cooldown on active service
    health checks to prevent hammering external APIs during prolonged outages.
    """

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        state_store: StateStore,
        admission_policy: AdmissionPolicy,
        maintenance_policy: MaintenancePolicy,
        cache_dir: str = ".cache/ingestion",
    ):
        """Initializes the TaskManager and provisions workspace roots.

        Args:
            exec_ctx: The global execution context for the run.
            state_store: Persistence layer for syncing job status to SQL.
            admission_policy: Logic for task queuing and deduplication.
            maintenance_policy: Logic for zombie detection and cleanup.
            cache_dir: Local path for the hot cache database.

        Raises:
            TypeError: If the ExecutionContext is not serializable for Ray.
        """
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

        # Decision: Probing Cooldown.
        self.PROBE_COOLDOWN_SEC = 300  # 5 minutes
        self._last_probe_ts: float = 0

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

    def _get_midnight_ts(self) -> float:
        """Calculates the Unix timestamp for 23:59:59 of the current day.

        Decision: Temporal Boundaries.
        Used to reconcile 'stuck' BLOCKED tasks. If a service outage lasts
        until midnight, we fail the tasks to prevent them from contaminating
        the next day's schedule.

        Returns:
            float: Unix timestamp for 23:59:59.
        """
        now = get_current_timestamp(strip_tz=True)
        midnight = now.replace(hour=23, minute=59, second=59, microsecond=0)
        return midnight.timestamp()

    @property
    def active_tasks(self) -> dict:
        """Expose active tasks mapping for the Janitor.

        Returns:
            dict: A mapping of Ray ObjectRefs to internal Task cache keys.
        """
        return self._active_tasks

    @property
    def compute(self) -> Compute:
        """Lazy-loaded compute resource coordinator.

        Decision: Lazy Loading.
        We only initialize the Compute/Ray manager when the first task
        is ready for dispatch to save memory on lightweight CLI runs.

        Returns:
            Compute: The resource manager for Ray worker lifecycle.
        """
        if self._compute is None:
            self._compute = Compute(self.exec_ctx)
        return self._compute

    def queue_tasks(self, task_ref: TaskRef, config_file_path: str) -> TaskRef | None:
        """Validates and adds a task to the persistent queue.

        Args:
            task_ref: Routing and identity handle for the task.
            config_file_path: Path to the serialized TaskContext.

        Returns:
            TaskRef | None: The queued reference, or None if admission failed.
        """
        if task_ref.status != ExecutionStatus.WAITING:
            task_ref = task_ref.with_updates(status=ExecutionStatus.WAITING)
        is_snapshot = "snapshot" in task_ref.identity.job_id.lower()
        meta = TaskMetadata.from_ref(
            task_ref,
            config_file_path,
            expires_at=get_end_of_day_ts() if is_snapshot else None,
        )
        if self.admission_policy.validate_and_queue(
            self.cache, self.lock, task_ref, meta
        ):
            LOG.info(
                "Queued Task", run_id=task_ref.identity.run_id, stage=task_ref.stage
            )
            return task_ref
        return None

    def _process_tasks(self) -> None:
        """Main dispatch loop: reconciles status and spawns workers.

        Decision: Priority Dispatch.
        We score tasks based on their Stage (e.g., ARCHIVE before EXTRACT)
        to ensure the system clears its local disk backlog before
        ingesting more data.
        """
        # Delegate resource cleanup to maintenance policy logic as well
        self.maintenance_policy._cleanup_finished_tasks(
            self._active_tasks, self.compute
        )

        if self.exec_ctx.stop_at_ts is not None:
            return

        # Active Recovery: Attempt to clear circuit breakers
        self.probe_blocked_services()

        self.compute.refresh_resources()

        valid_statuses = {
            ExecutionStatus.WAITING,
            ExecutionStatus.RETRY,
            ExecutionStatus.BLOCKED,
        }
        now_ts, midnight_ts = time.time(), self._get_midnight_ts()
        scored_keys = []
        for status in valid_statuses:
            for k in self.cache.iterkeys(
                pattern=f"{CACHE_TASK_NAMESPACE}:{status.value}:*"
            ):
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

            # Decision: Apply 'End of Day' failure to BLOCKED tasks.
            # If a service stays down past midnight, we transition the task
            # to FAILED so the Janitor can quarantine it for SRE review.
            # Decision: remarks are sent to JobUpdate, not TaskMetadata (cache).
            if task_ref.status == ExecutionStatus.BLOCKED and now_ts >= midnight_ts:
                LOG.error(
                    "Failing blocked task: outage exceeded midnight threshold.",
                    run_id=task_ref.identity.run_id,
                    service=task_meta.blocked_by,
                )
                remark = "Failed: Service outage persisted past midnight."
                self.state_store.update_run(
                    task_ref.identity.run_id,
                    JobUpdate(
                        JOB_ID=task_meta.job_id,
                        DATASET_ID=task_meta.dataset_id,
                        PARTITION_DATE=task_meta.partition_date,
                        JOB_STATUS=ExecutionStatus.FAILED.value,
                        REMARKS=remark,
                        LAST_UPDATED_AT_TS_LC=get_current_timestamp(
                            strip_tz=STRIP_TZ_FOR_DB
                        ).isoformat(sep=" "),
                    ),
                )
                self.cache.pop(key, None)
                failed_key = task_ref.build(status=ExecutionStatus.FAILED)
                task_meta.status = ExecutionStatus.FAILED.value
                self.cache[failed_key] = task_meta
                continue

            ready = (
                (task_ref.status == ExecutionStatus.WAITING)
                or (
                    task_ref.status == ExecutionStatus.RETRY
                    and now_ts >= task_meta.last_hb
                )
                or (
                    task_ref.status == ExecutionStatus.BLOCKED
                    and ServiceRegistry.is_healthy(task_meta.blocked_by or "")
                    and now_ts >= task_meta.last_hb
                )
            )

            if not ready:
                continue

            task_meta.status = ExecutionStatus.DISPATCHED.value
            task_meta.last_hb = time.time()
            new_key = task_ref.build(status=ExecutionStatus.DISPATCHED)
            ref = self.compute.spawn_worker(
                stage=StageName.from_label(task_ref.stage), key=new_key
            )

            if ref:
                self.state_store.update_run(
                    task_ref.identity.run_id,
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
                    run_id=task_ref.identity.run_id,
                    stage=task_ref.stage,
                )

    def _get_all_keys(self) -> list[str]:
        """Fetches a snapshot of all task keys from the cache.

        Returns:
            list[str]: All keys in the task namespace.
        """
        return list(self.cache.iterkeys(pattern=f"{CACHE_TASK_NAMESPACE}:*"))

    def recover_zombie_tasks(self) -> None:
        """Reclaims tasks in RUNNING state without active Ray workers.

        Decision: Implicit Recovery.
        By running this in the maintenance loop, the orchestrator
        self-heals from worker SIGKILLs without requiring manual
        intervention.
        """
        self.maintenance_policy.run(
            self.cache, self.lock, self._active_tasks, self.compute, self.exec_ctx
        )

    def probe_blocked_services(self) -> None:
        """Identifies unique blocked services and performs health probes.

        Decision: Active Recovery.
        Instead of waiting for a 5-minute TTL to expire, we actively probe
        services that are currently blocking the queue using a lightweight
        connection check.

        Decision: Throttled Probing.
        We enforce a PROBE_COOLDOWN_SEC to prevent excessive network traffic
        during long-duration infrastructure outages.
        """
        now = time.time()
        if now - self._last_probe_ts < self.PROBE_COOLDOWN_SEC:
            return

        self._last_probe_ts = now
        blocked_services = set()
        for k in self.cache.iterkeys(
            pattern=f"{CACHE_TASK_NAMESPACE}:{ExecutionStatus.BLOCKED.value}:*"
        ):
            meta = self.cache.get(k)
            if meta and meta.blocked_by:
                blocked_services.add(meta.blocked_by)

        if not blocked_services:
            return

        for svc_name in blocked_services:
            if not ServiceRegistry.is_healthy(svc_name):
                LOG.debug(f"Probing blocked service: {svc_name}")

                # Decision: Late Binding Fix.
                # We capture svc_name as a default argument (s_name) to ensure
                # the closure binds to the value at definition time,
                # preventing loop-variable leakage during asynchronous
                # execution or deferred evaluation.
                def _ping(s_name: str = svc_name) -> bool:
                    try:
                        svc = ServiceFactory.get_service(s_name)
                        if not hasattr(svc, "exists"):
                            raise NotImplementedError(
                                f"exists() method not implemented for service type: {s_name}"
                            )
                        return svc.exists("/")
                    except Exception:
                        return False

                ServiceRegistry.probe(svc_name, _ping)
