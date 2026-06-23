import time
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec
from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import Stage
from apps.ingestion.src.core.models.task import ExecutionStatus
from apps.ingestion.src.core.monitor import ServiceMonitor
from apps.ingestion.src.core.orchestrator.common.state import StateHub
from apps.ingestion.src.core.orchestrator.contracts.policies import (
    AdmissionPolicy,
    MaintenancePolicy,
)
from apps.ingestion.src.core.orchestrator.enums import TaskMetadata, TaskRef
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE
from apps.ingestion.src.utils.dates import end_of_day_timestamp
from filelock import FileLock
from libs.storage.cache.factory import get_cache
from libs.utils.dates import seconds_diff
from loguru import logger

from .compute import Compute
from .queue import TaskQueue

if TYPE_CHECKING:
    import ray

LOG = logger
PROBE_COOLDOWN_SECS = 3600


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
        state_store: StateHub,
        admission_policy: AdmissionPolicy,
        maintenance_policy: MaintenancePolicy,
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
        ServiceMonitor.setup(
            signal_dir=self.exec_ctx.signal_path,
            cache_config=self.exec_ctx.cache_config,
        )
        self.registry = ServiceMonitor()

        # CRITICAL: Create cache directory in the parent process BEFORE
        # spinning up Ray workers to prevent SQLite race conditions for diskcache.
        if self.exec_ctx.cache_config.get("type") == "diskcache":
            cache_filepath = self.exec_ctx.cache_config.get("filepath", ".cache")
            (self.exec_ctx.workspace_dir / cache_filepath).mkdir(
                parents=True, exist_ok=True
            )

        self.cache = get_cache(self.exec_ctx.cache_config)
        self.queue = TaskQueue(self.exec_ctx.task_queue_config)

        self._loop_counter = 0
        self.lock = FileLock(self.exec_ctx.lock_file)
        self.is_degraded = False
        self._compute: Compute | None = None

        # Track state to prevent log spamming on every tick
        self._last_summary_state: tuple[int, bool] = (0, False)

        # Decision: Probing Cooldown.
        self._last_probe_ts: float = 0

        try:
            self.exec_ctx.verify_serializable()
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

    # def _get_midnight_ts(self) -> float:
    #     """Calculates the Unix timestamp for 23:59:59 of the current day.

    #     Decision: Temporal Boundaries.
    #     Used to reconcile 'stuck' BLOCKED tasks. If a service outage lasts
    #     until midnight, we fail the tasks to prevent them from contaminating
    #     the next day's schedule.

    #     Returns:
    #         float: Unix timestamp for 23:59:59.
    #     """
    #     now = current_timestamp(naive=True)
    #     midnight = now.replace(hour=23, minute=59, second=59, microsecond=0)
    #     return midnight.timestamp()

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

    def enqueue(self, task_ref: TaskRef, config_file_path: str) -> TaskRef | None:
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
            expires_at=end_of_day_timestamp() if is_snapshot else None,
        )
        if self.admission_policy.admit(self.cache, self.lock, task_ref, meta):
            LOG.info(
                "Queued Task", run_id=task_ref.identity.run_id, stage=task_ref.stage
            )

            # Push to FlashQ with priority and group (concurrency control per job)
            self.queue.push(meta, task_ref.stage, task_ref.status)

            return task_ref
        return None

    def dispatch(self) -> None:
        """Main dispatch loop: reconciles resource status and spawns workers.

        Notes:
        - Higher priority on stages that reduce disk usage to ensure the
        system clears its local disk backlog before ingesting more data.
        """
        # Delegate resource cleanup to maintenance policy
        self.maintenance_policy.cleanup_tasks(self._active_tasks, self.compute)

        if self.exec_ctx.stop_at_ts is not None:
            return

        # Active Recovery: Attempt to clear circuit breakers
        self.probe_blocked_services()
        self.compute.refresh_resources()

        now_ts = time.time()
        dispatch_count = 0

        # Pop and process messages from FlashQ
        while True:
            msg = self.queue.pop(visibility_timeout=300)
            if not msg:
                break

            task_meta = self._decode_message(msg)
            if not task_meta:
                continue

            if not self._is_task_ready(task_meta, now_ts):
                continue

            if self._dispatch_task(task_meta, msg):
                dispatch_count += 1
            else:
                # System saturated, stop popping
                break

        if dispatch_count > 0:
            LOG.debug(f"Tick Summary: Dispatched {dispatch_count} tasks.")

    def _decode_message(self, msg) -> TaskMetadata | None:
        """Decode message data into TaskMetadata."""
        try:
            match msg.data:
                case dict():
                    return msgspec.convert(msg.data, type=TaskMetadata)
                case bytes():
                    return msgspec.json.decode(msg.data, type=TaskMetadata)
                case str():
                    return msgspec.json.decode(msg.data.encode(), type=TaskMetadata)
                case _:
                    LOG.error(f"Unsupported message data type: {type(msg.data)}")
                    return None
        except (msgspec.DecodeError, TypeError, ValueError) as e:
            LOG.error(f"Failed to decode message: {e}")
            return None

    def _is_task_ready(self, task_meta: TaskMetadata, now_ts: float) -> bool:
        """Check if task is ready for dispatch."""
        if task_meta.status != ExecutionStatus.RETRY:
            return True

        # For RETRY status, check if next_attempt_ts has passed
        if task_meta.next_attempt_ts is not None:
            if seconds_diff(task_meta.next_attempt_ts, now_ts) < 0:
                LOG.debug(
                    "Task is not ready for retry",
                    run_id=TaskMetadata.to_ref(task_meta).identity.run_id,
                )
                return False
            # Clear the retry timestamp since we're about to dispatch
            task_meta.next_attempt_ts = None

        return True

    def _dispatch_task(self, task_meta: TaskMetadata, msg) -> bool:
        """Dispatch a single task to a worker."""
        task_ref = TaskMetadata.to_ref(task_meta)

        # Update status for dispatch
        task_meta.status = ExecutionStatus.DISPATCHED.value
        task_meta.last_hb = time.time()
        new_key = task_ref.build(status=ExecutionStatus.DISPATCHED)

        # Spawn worker
        ref = self.compute.spawn_worker(
            stage=Stage(task_ref.stage),
            task_key=new_key,
            msg_id=msg.id,
        )

        if not ref:
            return False  # System saturated

        self._active_tasks[ref] = new_key
        LOG.success("Dispatching task", run_id=task_ref.identity.run_id)
        return True

    def recover_zombie_tasks(self) -> None:
        """Reclaims tasks in RUNNING state without active Ray workers.

        Decision: Implicit Recovery.
        By running this in the maintenance loop, the orchestrator
        self-heals from worker SIGKILLs without requiring manual
        intervention.
        """
        recovered = self.maintenance_policy.run(
            self.cache, self.lock, self._active_tasks, self.compute, self.exec_ctx
        )
        if recovered:
            for meta, stage in recovered:
                self.queue.push(meta, stage)
                LOG.info(f"Re-queued recovered zombie task: {meta.run_id}")

    def probe_blocked_services(self) -> None:
        """Identifies unique blocked services and performs health probes.

        Decision: Active Recovery.
        Instead of waiting for a 5-minute TTL to expire, we actively probe
        services that are currently blocking the queue using a lightweight
        connection check.

        Decision: Throttled Probing.
        We enforce a PROBE_COOLDOWN_SECS to prevent excessive network traffic
        during long-duration infrastructure outages.
        """
        now = time.time()
        if now - self._last_probe_ts < PROBE_COOLDOWN_SECS:
            return

        self._last_probe_ts = now
        blocked_info = defaultdict(str)  # service_name -> sample_task_key
        for k in set(
            self.cache.iterkeys(
                pattern=f"{CACHE_TASK_NAMESPACE}:{ExecutionStatus.BLOCKED.value}:*"
            )
        ):
            meta = self.cache.get(k)
            if meta and meta.blocked_by:
                blocked_info[meta.blocked_by] = k

        if not blocked_info:
            return

        for svc_name, task_key in blocked_info.items():
            if ServiceMonitor.is_healthy(svc_name):
                LOG.debug(f"Probing blocked service: {svc_name}")

                def _ping(s_name: str = svc_name, t_key: str = task_key) -> bool:
                    try:
                        svc = ServiceFactory.get(s_name)

                        # If this is a storage service, probe the specific resource that blocked us
                        if ServiceFactory.is_file_source(s_name):
                            meta = self.cache.get(t_key)
                            if meta:
                                from apps.ingestion.src.core.contexts.task import (
                                    load_context,
                                )

                                ctx = load_context(Path(meta.config_file).parent)
                                if ctx and ctx.extract and ctx.extract.resource:
                                    LOG.trace(
                                        f"Probing resource {ctx.extract.resource} for {s_name}"
                                    )
                                    return svc.exists(ctx.extract.resource)

                        if not hasattr(svc, "exists"):
                            raise NotImplementedError(
                                f"exists() method not implemented for "
                                f"service type: {s_name}"
                            )
                        return svc.exists("/")
                    except Exception:
                        return False

                if ServiceMonitor.probe(svc_name, _ping):
                    # Re-queue tasks for the recovered service
                    task_ref = TaskRef.from_key(task_key)
                    meta = self.cache.get(task_key)
                    self.queue.push(meta, task_ref.stage, ExecutionStatus.BLOCKED)
                    LOG.info(f"Re-queued blocked task: {task_ref.identity.run_id}")
