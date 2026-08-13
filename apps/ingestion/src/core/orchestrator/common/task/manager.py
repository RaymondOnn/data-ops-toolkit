# 572 -> 424
import time
from pathlib import Path

import ray
from filelock import FileLock
from libs.utils.dates import seconds_diff
from loguru import logger

from src.core.contexts import ExecutionContext
from src.core.models.task import ExecutionStatus, Task
from src.core.models.task.enums import TaskIdentity, TaskRef
from src.core.orchestrator.common.state import StateHub
from src.core.orchestrator.common.task.compute import Compute
from src.core.orchestrator.common.task.executor import process_stage_task
from src.core.orchestrator.common.task.queue import TaskQueue
from src.core.orchestrator.common.timeout import TimeoutMonitor
from src.core.orchestrator.contracts.policies import (
    AdmissionPolicy,
    MaintenancePolicy,
)
from src.core.orchestrator.enums import TaskMetadata
from src.core.stages.contracts.stage import DISK_FREE_STAGES
from src.core.stages.enums import Stage
from src.services.factory import ServiceFactory
from src.services.health import ServiceMonitor, SystemMonitor
from src.utils.common import short_hash
from src.utils.constants import CACHE_TASK_NAMESPACE
from src.utils.dates import end_of_day_timestamp
from src.utils.exceptions import (
    OutOfDiskSpace,
    RollbackRequired,
    TryAgainLater,
)

from .cache import TaskCache
from .outcome import (
    BlockedOutcome,
    FailedOutcome,
    ProgressOutcome,
    RetryOutcome,
    RollbackOutcome,
    SuccessOutcome,
    is_complete,
    is_retryable,
)

LOG = logger
PROBE_COOLDOWN_SECS = 3600


class TaskManager:
    """Orchestration Control Plane for task queuing and dispatch.

    Decision: Centralized Resource Awareness.
    The TaskManager combines logical admission control (AdmissionPolicy),
    hardware resource management (Compute), and external health (Registry)
    to ensure tasks are only dispatched when the environment is ready.
    """

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        state_store: StateHub,
        timeout_monitor: TimeoutMonitor,
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
        self.timeout = timeout_monitor
        self.admission_policy = admission_policy
        self.maintenance_policy = maintenance_policy

        # Decentralized: TaskManager owns the active and data zones
        self.exec_ctx.active_path.mkdir(parents=True, exist_ok=True)
        self.exec_ctx.data_path.mkdir(parents=True, exist_ok=True)

        # 2. Initialize the Global Registry (Diskcache)
        # This ensures the shared cache path exists for all Ray workers
        cache_config = self.exec_ctx.cache_config
        ServiceMonitor.setup(
            signal_dir=self.exec_ctx.signal_path,
            cache_config=cache_config,
        )
        self.registry = ServiceMonitor()
        self.cache = TaskCache(
            cache_config=cache_config,
            prefix=f"{CACHE_TASK_NAMESPACE}:",
            state_hub=self.state_store,
        )
        self.queue = TaskQueue(self.exec_ctx.task_queue_config)
        self.system = SystemMonitor(self.exec_ctx.workspace_dir)

        self._loop_counter = 0
        self.lock = FileLock(self.exec_ctx.lock_file)
        self._compute: Compute | None = None

        # Track state to prevent log spamming on every tick
        self._last_summary_state: tuple[int, bool] = (0, False)

        # Decision: Probing Cooldown.
        self._last_probe_ts: float = 0

        # Track disk recovery attempts
        self._disk_recovery_attempts: int = 0

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
        # Local tracker for active Ray tasks (run_id -> ObjectRef)
        self._active_tasks: dict[str, ray.ObjectRef] = {}

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
    def active_tasks(self) -> dict[str, ray.ObjectRef]:
        """Expose active tasks mapping for the Janitor.

        Returns:
            dict: A mapping of Ray ObjectRefs to internal Task cache keys.

        Notes:
        - Blocked tasks are held in the 'active/' workspace to allow for seamless
        recovery once infrastructure outages are resolved.
        """
        return self._active_tasks

    @property
    def compute(self) -> Compute:
        """Lazy-loaded compute resource coordinator.

        Returns:
            Compute: The resource manager for Ray worker lifecycle.
        """
        # Lazy loaded to save memory on lightweight CLI runs
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
            task_ref = task_ref.with_updates(
                status=ExecutionStatus.WAITING,
            )

        # Load task context
        is_snapshot = "append" in task_ref.identity.job_id.lower()

        # Create timeout state
        timeout_state = self.timeout.create_timeout_state(task_ref=task_ref)

        meta = TaskMetadata.from_ref(
            task_ref,
            config_file_path,
            expires_at=end_of_day_timestamp() if is_snapshot else None,
        )
        meta.scheduled_at = time.time()
        meta.timeout_state = timeout_state

        if self.admission_policy.admit(self.cache, self.lock, meta):
            LOG.info(
                "Queued Task",
                run_id=task_ref.identity.run_id,
                step_id=task_ref.step_id,
            )

            # Push to queue with priority and group (concurrency control per job)
            self.queue.push(meta)

            return task_ref
        return None

    def dispatch(self) -> None:
        """Main dispatch loop: reconciles resource status and spawns workers.

        Notes:
        - Higher priority on stages that reduce disk usage to ensure the
        system clears its local disk backlog before ingesting more data.
        """

        def _score_task(item: tuple[str, TaskMetadata]):
            return self.queue.calculate_priority(item[1])

        self.reconcile_active_tasks()

        now_ts = time.time()

        # Check if we're in shut down mode
        if self.exec_ctx.stop_at_ts is not None:
            LOG.trace("[DISPATCH] shut down", reason="stop_at_ts")
            return

        LOG.trace("[DISPATCH] cycle start", queue_size=self.queue.backend.size())

        if self.queue.is_empty():
            LOG.trace(
                "[DISPATCH] Task queue is empty. Skipping current dispatch cycle..."
            )
            return

        dispatch_count = 0

        queue_items = list(self.queue.items())
        queue_items.sort(key=_score_task)
        for msg_id, metadata in queue_items:
            if metadata.run_id in self.active_tasks:
                continue

            if not self._is_task_ready(metadata, now_ts):
                LOG.info(
                    "Task is not ready for dispatch",
                    run_id=metadata.run_id,
                    stage=metadata.current_step_id,
                )
                continue

            # Constraint 1: Check standard compute pool allocation (CPU/RAM thresholds via Compute)
            stage_enum = Stage(metadata.current_stage)
            if not self.compute.has_capacity(stage_enum):
                LOG.trace(
                    f"Compute saturation reached. Pausing scheduling ring at step: {metadata.current_step_id}"
                )
                break

            popped = self.queue.pop_by_key(msg_id)
            if not popped:
                continue
            p_msg_id, p_metadata = popped

            self._dispatch_task(task_meta=p_metadata, msg_id=p_msg_id)
            dispatch_count += 1

        if dispatch_count > 0:
            LOG.debug(f"Tick Summary: Dispatched {dispatch_count} tasks.")

    def _is_task_ready(self, task_meta: TaskMetadata, now_ts: float) -> bool:
        """Check if task is ready for dispatch."""
        # Only allow disk-light tasks
        if (
            task_meta.current_stage not in DISK_FREE_STAGES
            and self.system.is_disk_blocked()
        ):
            LOG.warning(
                "Blocked disk-intensive task due to disk pressure",
                run_id=task_meta.run_id,
            )
            return False
        LOG.trace("Disk Space Check: PASSED")

        # SCHEDULE_TO_START warning (not failure)
        elapsed = now_ts - task_meta.scheduled_at
        threshold = self.timeout.get_schedule_warning_threshold(
            task_meta.current_step_id
        )
        if elapsed > threshold:
            # cache_key = TaskMetadata.generate_cache_key()
            # if cache_key not in self._schedule_warning_logged:
            LOG.warning(
                "Task exceeded SCHEDULE_TO_START threshold",
                run_id=task_meta.run_id,
                step=task_meta.current_step_id,
                waited_seconds=elapsed,
                threshold_seconds=threshold,
            )
            # self._schedule_warning_logged.add(cache_key)
        LOG.trace("Queue Age Check: PASSED")

        # Early Exit: Check for RETRY or BLOCKED status
        if task_meta.status not in (
            ExecutionStatus.RETRY,
            ExecutionStatus.BLOCKED.value,
        ):
            return True

        # Check for BLOCKED status - service outage
        if task_meta.blocked_by:
            if ServiceMonitor.is_healthy(task_meta.blocked_by):
                return True
            return self._service_has_recovered(task_meta)
        LOG.trace("Blocked Tasks Check: SKIPPED")

        # For RETRY status, check if next_attempt_ts has passed
        if task_meta.next_attempt_ts is not None:
            if seconds_diff(task_meta.next_attempt_ts, now_ts) < 0:
                LOG.debug(
                    "Task is not ready for retry",
                    run_id=task_meta.run_id,
                )
                return False
            # Clear the retry timestamp since we're about to dispatch
            task_meta.next_attempt_ts = None
        LOG.trace("Retry Task Check: PASSED")

        return True

    def _dispatch_task(self, task_meta: TaskMetadata, msg_id) -> None:
        """Dispatch a single task to a worker."""

        LOG.trace(
            "[DISPATCH] attempting",
            run_id=task_meta.run_id,
            step=task_meta.current_step_id,
        )

        # Update the registry / cache with the UPDATED metadata
        with self.lock:
            new_key = self.cache.transition_state(
                task_meta, next_status=ExecutionStatus.DISPATCHED
            )

        def _cleanup_dispatch_state(future=None) -> None:
            self._active_tasks.pop(task_meta.run_id, None)

        # Submit to ray cluster
        ref = process_stage_task.remote(
            f"{task_meta.current_step_id}_{short_hash(8)}",
            self.exec_ctx,
            task_meta.generate_cache_key(),
        )
        self.active_tasks[task_meta.run_id] = ref
        LOG.success("Dispatching task", run_id=task_meta.run_id)
        LOG.trace(
            "[DISPATCH] spawn succeeded",
            run_id=task_meta.run_id,
            ref=str(ref),
            new_key=new_key,
        )

        # 3. Acknowledge and clear the message from the queue immediately
        # if hasattr(ref, "_on_completed"):
        #     ref._on_completed(_cleanup_dispatch_state)

        if msg_id:
            self.queue.ack(msg_id)

    def reconcile_active_tasks(self) -> None:
        """Polls active Ray tasks, retrieves results, and executes DB state transitions."""
        if not self.active_tasks:
            return

        # Check which Ray worker tasks have completed (non-blocking)
        ready_refs, _ = ray.wait(
            list(self.active_tasks.values()),
            num_returns=len(self.active_tasks),
            timeout=0.0,
        )

        for ref in ready_refs:
            # Find which run_id this completed future belongs to
            run_id = None
            for rid, rref in list(self.active_tasks.items()):
                if rref == ref:
                    run_id = rid
                    break

            if not run_id:
                continue

            # Remove from active map immediately
            self.active_tasks.pop(run_id, None)

            try:
                # 1. Fetch the returned task dictionary from the worker
                result = ray.get(ref)
                metadata: TaskMetadata = result["metadata"]

                # Handle cases where the worker crashed before metadata was loaded
                if not metadata:
                    LOG.error(
                        f"Worker process failed during bootstrap for run {run_id}. Forcing failure."
                    )
                    # Reconstruct a generic Task to trigger the FailedOutcome handler
                    fallback_ref = TaskRef(
                        step_id="start",
                        status=ExecutionStatus.RUNNING,
                        identity=TaskIdentity(
                            job_id="unknown_job",
                            dataset_id="unknown_dataset",
                            partition_date="unknown_date",
                            run_id=run_id,
                        ),
                    )
                    task = Task(
                        task_ref=fallback_ref,
                        worker_id="driver-reconciler",
                        exec_ctx=self.exec_ctx,
                    )
                    self._conclude_task(
                        task,
                        runtime_exception=result.get("error")
                        or RuntimeError("Worker bootstrap crash"),
                    )
                    continue

                # Reconstruct the Task instance for the outcome handlers
                task_ref = TaskRef.from_key(metadata.generate_cache_key())
                task = Task(
                    task_ref=task_ref,
                    worker_id="driver-reconciler",
                    exec_ctx=self.exec_ctx,
                )

                # 3. Process the outcome on the driver side (writes DB telemetry & re-queues)
                if result["success"]:
                    self._conclude_task(task, runtime_exception=None)
                else:
                    self._conclude_task(task, runtime_exception=result["error"])

            except Exception:
                LOG.exception(f"Error reconciling completed worker run {run_id}")

    def _conclude_task(
        self, task: Task, runtime_exception: Exception | None = None
    ) -> None:
        """Finalizes a stage execution and calculates the next state."""
        if runtime_exception:
            if isinstance(runtime_exception, RollbackRequired):
                RollbackOutcome().handle(self, task, runtime_exception)
            elif isinstance(runtime_exception, OutOfDiskSpace | TryAgainLater):
                BlockedOutcome().handle(self, task, runtime_exception)
            elif is_retryable(task, runtime_exception):
                RetryOutcome().handle(self, task, runtime_exception)
            else:
                FailedOutcome().handle(self, task, runtime_exception)
            return

        # Success Evaluations
        if is_complete(task):
            SuccessOutcome().handle(self, task)
        else:
            ProgressOutcome().handle(self, task, None)

    def recover_zombie_tasks(self) -> None:
        """Reclaims tasks in RUNNING state without active Ray workers.

        Notes:
        - By running this in the maintenance loop, the orchestrator
        self-heals from worker SIGKILLs without requiring manual
        intervention.
        """
        recovered = self.maintenance_policy.run(
            self.cache, self.lock, self.active_tasks, self.exec_ctx
        )
        if recovered:
            for meta in recovered:
                self.queue.push(meta)
                LOG.info(f"Re-queued recovered zombie task: {meta.run_id}")

    def _service_has_recovered(self, metadata: TaskMetadata) -> bool:
        """Determines if a structural external dependency block has cleared."""
        if not metadata.blocked_by:
            return True

        svc_name = metadata.blocked_by

        # Native helper logic acting as an external ping hook wrapper
        def _ping() -> bool:
            try:
                from src.core.contexts.task import load_context

                ctx = load_context(Path(metadata.config_file).parent)
                if not ctx:
                    return False

                # 1. Resolve connection configuration
                config = metadata.current_step.config

                conn_config = {}
                if config and config.connection:
                    conn_config = config.connection

                # 2. Retrieve target resource path (if any)
                target_resource = config.resource if config else None

                # 3. Instantiate Service
                if conn_config:
                    svc = ServiceFactory.get(**conn_config)
                else:
                    svc = ServiceFactory.get(type=svc_name)

                # 4. Delegate target probe logic directly to instance check
                if ServiceFactory.is_file_source(svc):
                    return svc.probe(target=target_resource)

                return svc.probe()

            except Exception as e:
                LOG.debug(f"Probe execution error for {svc_name}: {e}")
                return False

        return ServiceMonitor.probe(svc_name, _ping)
