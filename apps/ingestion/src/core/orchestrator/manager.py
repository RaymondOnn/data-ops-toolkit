import time

import msgspec
import ray
from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import (
    STAGE_TERMINAL_SENTINEL,
    StageName,
)
from apps.ingestion.src.core.models.states import ZombieState
from apps.ingestion.src.core.models.task import ExecutionStatus, Task, TaskSignal
from apps.ingestion.src.core.orchestrator.compute import Compute
from apps.ingestion.src.services.registry import ServiceRegistry
from apps.ingestion.src.utils.common import find_path
from apps.ingestion.src.utils.constants import (
    CACHE_TASK_NAMESPACE,
    MANIFEST_FILENAME,
    STRIP_TZ_FOR_DB,
)
from apps.ingestion.src.utils.dates import get_end_of_day_ts
from filelock import FileLock
from libs.cache.utils import get_cache
from libs.utils.dates import get_current_timestamp
from loguru import logger

from .enums import JobUpdate, TaskMetadata, TaskRef
from .state import StateStore

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
        cache_dir: str = ".cache/ingestion",
    ):
        self.exec_ctx = exec_ctx
        self.state_store = state_store
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

        # Track state to prevent log spamming on every tick
        self._last_summary_state: tuple[int, bool] = (0, False)

        # Initialize the Resource Coordinator for cost and priority logic
        self.compute = Compute(self.exec_ctx)

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

    def queue_tasks(
        self,
        task_ref: TaskRef,
        config_file_path: str,
    ) -> TaskRef | None:  # Return the TaskRef for Orchestrator to use
        """Checks config, creates tasks if not in cache, and submits them."""
        # 1. Transition the key to WAITING status for the queue
        if task_ref.status != ExecutionStatus.WAITING.value:
            task_ref = task_ref.with_updates(status=ExecutionStatus.WAITING.value)

        queue_key = task_ref.build()

        # 2. GUARD: Use TaskRef for cleaner duplicate detection
        pattern = f"{CACHE_TASK_NAMESPACE}:*:*:{task_ref.identifier}:*"
        existing_key_str = next(iter(self.cache.iterkeys(pattern=pattern)), None)

        if existing_key_str:
            existing_ref = TaskRef.from_str(existing_key_str)
            LOG.warning(
                "Partition already has an active run. Skipping queue.",
                identifier=task_ref.identifier,
                active_run_id=existing_ref.run_id,
            )
            return None

        is_snapshot = "snapshot" in task_ref.job_id.lower()
        expires_at = get_end_of_day_ts() if is_snapshot else None

        # 3. Use t_key properties to populate metadata and state updates
        meta = TaskMetadata.from_ref(task_ref, config_file_path, expires_at=expires_at)

        with self.lock:
            self.cache[queue_key] = meta

        # Emit WAITING state to database
        self.state_store.update_run(
            task_ref.run_id,
            JobUpdate(
                JOB_ID=task_ref.job_id,
                DATASET_ID=task_ref.dataset_id,
                PARTITION_DATE=task_ref.partition_date,
                JOB_STATUS=ExecutionStatus.WAITING.value,
                CURRENT_STAGE=task_ref.stage,
                LAST_UPDATED_AT_TS_LC=get_current_timestamp(
                    strip_tz=STRIP_TZ_FOR_DB
                ).isoformat(sep=" "),
            ),
        )

        LOG.info(
            "Queued Task",
            job_id=task_ref.job_id,
            run_id=task_ref.run_id,
            stage=task_ref.stage,
        )

        return task_ref

    def _process_tasks(self) -> None:
        # 1. Instruct Compute to snapshot current cluster resources for this tick
        self._cleanup_finished_tasks()
        self.compute.refresh_resources()

        # Scan for tasks that are ready for processing (WAITING, RETRY, or BLOCKED)
        valid_statuses = {
            ExecutionStatus.WAITING.value,
            ExecutionStatus.RETRY.value,
            ExecutionStatus.BLOCKED.value,
        }

        # Priority Sorting: Terminal stages jump to the front of the queue
        scored_keys = []
        # Optimization: Scan only for actionable statuses to avoid O(N) cache pressure
        for status in valid_statuses:
            for k in self.cache.iterkeys(pattern=f"{CACHE_TASK_NAMESPACE}:{status}:*"):
                try:
                    ref_candidate = TaskRef.from_str(k)
                    stage_enum = StageName.from_label(ref_candidate.stage)
                    priority = STAGES_PRIORITY.get(stage_enum, 0)
                    scored_keys.append((priority, k))
                except ValueError:
                    continue

        # Sort descending by priority score
        candidate_keys = [k for _, k in sorted(scored_keys, reverse=True)]

        dispatch_count = 0

        for key in candidate_keys:
            task_ref = TaskRef.from_str(key)
            stage_enum = StageName.from_label(task_ref.stage)

            # Only lock when we have a potential candidate to update
            task_meta: TaskMetadata = self.cache.get(key)
            if not task_meta:
                continue

            is_waiting = task_ref.status == ExecutionStatus.WAITING.value
            # Case 1: Normal Retry (Timer based)
            is_retry_ready = (
                task_ref.status == ExecutionStatus.RETRY.value
                and time.time() >= task_meta.last_hb
            )
            # Case 2: Service Outage (Signal based)
            is_blocked_ready = (
                task_ref.status == ExecutionStatus.BLOCKED.value
                and ServiceRegistry.is_healthy(task_meta.blocked_by or "")
                and time.time() >= task_meta.last_hb
            )

            if not (is_waiting or is_retry_ready or is_blocked_ready):
                continue

            # 1. Update status to DISPATCHED before building the new key
            # This ensures the cache key reflects the transition correctly.
            task_meta.status = ExecutionStatus.DISPATCHED.value
            task_meta.last_hb = time.time()

            new_key = task_ref.build(status=task_meta.status)

            # 2. Pass the NEW authoritative key to the worker
            ref = self.compute.spawn_worker(stage=stage_enum, key=new_key)

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
                    # Commit the state transition to cache only if spawn was successful
                    self.cache.pop(key, None)
                    self.cache[new_key] = task_meta

                # Track the ObjectRef to know when the task finishes
                self._active_tasks[ref] = new_key
                dispatch_count += 1
                LOG.success(
                    "Dispatching dynamic task",
                    job_id=task_meta.job_id,
                    run_id=task_meta.run_id,
                    stage=task_ref.stage,
                )

        # STATEFUL LOGGING: Only log if pending count or capacity status changed
        has_pending = len(candidate_keys) > 0
        is_stuck = has_pending and dispatch_count == 0
        current_state = (len(candidate_keys), is_stuck)

        if current_state != self._last_summary_state:
            if is_stuck:
                LOG.info(
                    f"Tick Summary: {len(candidate_keys)} tasks pending, "
                    "but system is at capacity. Waiting for resources..."
                )
            self._last_summary_state = current_state

    def _get_all_keys(self) -> list[str]:
        """
        Fetches a snapshot of all task keys from the cache.
        """
        return list(self.cache.iterkeys(pattern=f"{CACHE_TASK_NAMESPACE}:*"))

    # TODO: Handle Timeouts
    def recover_zombie_tasks(self) -> None:
        """Scans all stage queues for zombie tasks."""
        # 1. Proactive Reclamation: Check for naturally finished Ray tasks
        self._cleanup_finished_tasks()

        # 2. Self-healing: Reconcile Ray actor counts with logical tracking
        self.compute.reconcile_counts()

        dispatched_statuses = {s.value for s in ExecutionStatus.dispatched_statuses()}
        for key in self.cache.iterkeys(pattern=f"{CACHE_TASK_NAMESPACE}:*:*"):
            try:
                task_ref = TaskRef.from_str(key)
            except ValueError:
                continue

            if task_ref.status not in dispatched_statuses:
                continue

            meta: TaskMetadata = self.cache.get(key)
            if not meta:
                continue

            try:
                stage_enum = StageName.from_label(task_ref.stage)
            except ValueError:
                continue

            # --- Delegate to Inferred Expert ---
            is_zombie = ZombieState.is_applicable(
                cache_key=key,
                meta=meta,
                active_ray_tasks=self._active_tasks,
                exec_ctx=self.exec_ctx,
            )

            if is_zombie:
                # 1. Respect Self-Healing Disable
                if self.exec_ctx.disable_self_healing:
                    LOG.error(
                        "Zombie detected | Self-healing disabled. Failing task.",
                        key=key,
                    )
                    task = Task(
                        task_ref=task_ref,
                        worker_id="engine-recovery",
                        exec_ctx=self.exec_ctx,
                    )
                    task.update_manifest(
                        {
                            "status": ExecutionStatus.FAILED,
                            "remarks": "Zombie Task detected",
                        }
                    )
                    task.request_status_sync(TaskSignal.FAIL)
                    task.move_to_folder("FAILED")
                    with self.lock:
                        self.cache.pop(key, None)
                    continue

                active_path = find_path(self.exec_ctx.active_path, meta.run_id)
                manifest_file = active_path / MANIFEST_FILENAME if active_path else None

                if manifest_file and manifest_file.exists():
                    mtime = manifest_file.stat().st_mtime
                    if time.time() - mtime < 300:
                        # Physical heartbeat is fresh!
                        meta.last_hb = time.time()
                        with self.lock:
                            self.cache[key] = meta
                            continue

                    # Reclaim resources for the zombie task
                    for ref, active_key in list(self._active_tasks.items()):
                        if active_key == key:
                            self.compute.reclaim_resources(ref)
                            self._active_tasks.pop(ref)  # Remove from local tracker

                    LOG.warning("Zombie task detected", key=key)
                    self._recover_task(key)

    def _recover_task(self, key: str) -> None:
        """
        Recovers a stalled task by checking its physical progress.
        """
        task_ref = TaskRef.from_str(key)

        # .get() returns Optional[Any], so we check type and None-ness at once
        task_meta = self.cache.get(key)
        if not isinstance(task_meta, TaskMetadata):
            return

        # 1. Verify if the stage actually finished on disk
        if self._check_stage_completion_on_disk(task_ref.run_id, task_ref.stage):
            # Promotion: Find the next stage label
            next_stage = StageName.next(task_ref.stage)

            LOG.info(
                "Recovery: Step was successful on disk. Promoting.",
                run_id=task_ref.run_id,
                from_stage=task_ref.stage,
                to_stage=next_stage,
            )

            with self.lock:
                self.cache.pop(key, None)
                if next_stage != STAGE_TERMINAL_SENTINEL:
                    new_key = task_ref.build(
                        status=ExecutionStatus.WAITING.value, stage=next_stage
                    )
                    task_meta.status = ExecutionStatus.WAITING.value
                    self.cache[new_key] = task_meta
        else:
            # If no symlink exists, the worker died mid-stream or before finalize.
            # Reset to WAITING in the SAME queue to allow a retry.
            LOG.info(
                "Recovery: No physical proof of success. Resetting for retry.",
                run_id=task_ref.run_id,
                stage=task_ref.stage,
            )

            # Rehydrate the Task to perform a proper manifest reset
            task = Task(
                task_ref=task_ref,
                worker_id="engine-recovery",
                exec_ctx=self.exec_ctx,
            )

            task.update_manifest(
                {
                    "status": ExecutionStatus.WAITING.value,
                    "current_stage": None,
                }
            )

            # Sync the engine cache with the new manifest state
            # Clear indicators and set to WAITING
            (task.folder / ".retrying").unlink(missing_ok=True)
            (task.folder / ".blocked").unlink(missing_ok=True)
            with self.lock:
                self.cache.pop(key, None)
                task_meta.status = ExecutionStatus.WAITING.value
                task_meta.last_hb = time.time()

                # Reclaim resources if there was an active Ray worker for this key
                for ref, active_key in list(self._active_tasks.items()):
                    if active_key == key:
                        self.compute.reclaim_resources(ref)
                        self._active_tasks.pop(ref)
                        break

                new_key = task_ref.build(status=ExecutionStatus.WAITING.value)
                self.cache[new_key] = task_meta

    def _check_stage_completion_on_disk(self, run_id: str, stage_name: str) -> bool:
        """
        Checks if the 'active' symlink for this stage exists.
        This is the definitive proof of success in our new structure.
        """
        active_path = find_path(self.exec_ctx.active_path, run_id)
        if not active_path:
            return False

        manifest_path = active_path / MANIFEST_FILENAME
        if not manifest_path.exists():
            return False

        try:
            # Definitive proof: The stage results are recorded in the manifest
            with manifest_path.open("rb") as f:
                # We use a generic dict decode here to check key existence
                manifest_data = msgspec.json.decode(f.read())
                return manifest_data.get(stage_name) is not None
        except Exception:
            LOG.error("Failed to read manifest during recovery check", run_id=run_id)
            return False

    def _cleanup_finished_tasks(self) -> None:
        """Checks for finished Ray tasks and triggers resource reclamation."""
        if not self._active_tasks:
            return

        # Non-blocking check: find which refs are done
        ready_refs, _ = ray.wait(list(self._active_tasks.keys()), timeout=0)

        for ref in ready_refs:
            # Pop from Manager's local tracking
            self._active_tasks.pop(ref)

            # Tell Compute to free up the budget
            self.compute.reclaim_resources(ref)
