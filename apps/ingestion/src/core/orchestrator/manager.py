import time

import msgspec
import ray
from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.models.stages.enums import (
    STAGE_TERMINAL_SENTINEL,
    StageName,
)
from apps.ingestion.src.core.models.task import ExecutionStatus, Task
from apps.ingestion.src.core.orchestrator.compute import Compute
from apps.ingestion.src.services.registry import ServiceRegistry
from apps.ingestion.src.utils.common import find_path
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE, MANIFEST_FILENAME
from apps.ingestion.src.utils.dates import epoch_to_iso, get_end_of_day_ts
from filelock import FileLock
from libs.cache.utils import get_cache
from loguru import logger

from .enums import TaskMetadata

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
    def __init__(self, exec_ctx: ExecutionContext, cache_dir: str = ".cache/ingestion"):
        self.exec_ctx = exec_ctx
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

    def run(self) -> None:
        """Main loop managing multiple tasks."""
        LOG.info("Starting Orchestrator...")
        while True:
            self.recover_zombie_tasks()
            self._process_tasks()
            time.sleep(10)  # Frequency of polling

    def queue_tasks(
        self,
        identifier: str,
        run_id: str,
        config_file_path: str,
        current_stage: str | None = None,
    ) -> None:
        """Checks config, creates tasks if not in cache, and submits them."""
        # Always start in the 'start' queue
        current_stage = current_stage or "start"
        queue_key = (
            f"{CACHE_TASK_NAMESPACE}:{ExecutionStatus.PENDING.value}:{current_stage}:"
            f"{identifier}:{run_id}"
        )
        job_id, dataset_id, partition_date = identifier.split(":")

        # ?: Logic to determine if this is a snapshot (e.g., based on
        # job naming convention)
        is_snapshot = "snapshot" in job_id.lower()
        expires_at = get_end_of_day_ts() if is_snapshot else None

        # Log the dispatch with human-readable timestamps
        expiry_str = epoch_to_iso(expires_at) if expires_at else "NEVER"

        # Check if task exists in cache
        if queue_key not in self.cache:
            # Brand new entry
            meta = TaskMetadata(
                job_id=job_id,
                run_id=run_id,
                dataset_id=dataset_id,
                partition_date=partition_date,
                status=ExecutionStatus.PENDING.value,
                config_file=config_file_path,
                current_stage=current_stage,
                last_hb=time.time(),
                expires_at=expires_at,
            )
            with self.lock:
                self.cache[queue_key] = meta
                # Set the lookup key so Orchestrator can find the run_id for this date
                self.cache[f"active_run:{identifier}"] = run_id

            LOG.info(
                "Queued Task",
                job_id=job_id,
                run_id=run_id,
                stage=current_stage,
                expires=expiry_str,
            )

    def _process_tasks(self) -> None:
        # 1. Instruct Compute to snapshot current cluster resources for this tick
        self._cleanup_finished_tasks()
        self.compute.refresh_resources()

        # Scan for tasks that are ready for processing (PENDING, RETRY, or BLOCKED)
        candidate_patterns = [
            f"{CACHE_TASK_NAMESPACE}:PENDING:*",
            f"{CACHE_TASK_NAMESPACE}:RETRY:*",
            f"{CACHE_TASK_NAMESPACE}:BLOCKED:*",
        ]
        candidate_keys = []
        for pattern in candidate_patterns:
            candidate_keys.extend(self.cache.iterkeys(pattern=pattern))

        # Priority Sorting: Terminal stages jump to the front of the queue
        scored_keys = []
        for k in candidate_keys:
            try:
                stage_enum = StageName.from_label(k.split(":")[2])
                priority = STAGES_PRIORITY.get(stage_enum, 0)
            except ValueError:
                priority = 0
            scored_keys.append((priority, k))

        # Sort descending by priority score
        candidate_keys = [k for _, k in sorted(scored_keys, reverse=True)]

        dispatch_count = 0

        for key in candidate_keys:
            parts = key.split(":")
            status = parts[1]
            stage_label = parts[2]
            identifier = ":".join(parts[3:6])
            run_id = parts[6]

            stage_enum = StageName.from_label(stage_label)

            # Only lock when we have a potential candidate to update
            task_meta: TaskMetadata = self.cache.get(key)
            if not task_meta:
                continue

            is_pending = status == ExecutionStatus.PENDING.value
            # Case 1: Normal Retry (Timer based)
            is_retry_ready = (
                status == ExecutionStatus.RETRY.value
                and time.time() >= task_meta.last_hb
            )
            # Case 2: Service Outage (Signal based)
            is_blocked_ready = (
                status == ExecutionStatus.BLOCKED.value
                and ServiceRegistry.is_healthy(task_meta.blocked_by or "")
                and time.time() >= task_meta.last_hb
            )

            if not (is_pending or is_retry_ready or is_blocked_ready):
                continue

            if is_pending or is_retry_ready or is_blocked_ready:
                # Prepare metadata for dispatch
                task_meta.status = ExecutionStatus.QUEUED.value
                task_meta.last_hb = time.time()
                new_key = (
                    f"{CACHE_TASK_NAMESPACE}:{task_meta.status}:{stage_label}:"
                    f"{identifier}:{run_id}"
                )

                # Pass the 'key' from the loop into the spawn_worker method
                ref = self.compute.spawn_worker(stage=stage_enum, key=new_key)

            if ref:
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
                    stage=stage_label,
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

        dispatched_statuses = {
            s.value for s in ExecutionStatus.dispatched_statuses()
        }
        for key in self.cache.iterkeys(pattern=f"{CACHE_TASK_NAMESPACE}:*:*"):
            parts = key.split(":")
            status = parts[1]
            if status not in dispatched_statuses:
                continue

            meta: TaskMetadata = self.cache.get(key)
            if not meta:
                continue

            try:
                stage_label = parts[2]
                stage_enum = StageName.from_label(stage_label)
            except ValueError:
                continue

            if time.time() - meta.last_hb > 300:
                # Check the 'Physical Heartbeat' (Manifest timestamp)
                # before declaring it a zombie.
                active_path = find_path(self.exec_ctx.active_path, meta.run_id)
                manifest_file = (
                    active_path / MANIFEST_FILENAME if active_path else None
                )

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
        parts = key.split(":")
        stage_label = parts[2]
        identifier = ":".join(parts[3:6])
        run_id = parts[6]

        # .get() returns Optional[Any], so we check type and None-ness at once
        task_meta = self.cache.get(key)
        if not isinstance(task_meta, TaskMetadata):
            return

        # 1. Verify if the stage actually finished on disk but failed to transit
        # We check for the .success marker in the current stage's folder
        if self._check_stage_completion_on_disk(run_id, stage_label):
            # Promotion: Find the next stage label
            next_stage = StageName.next(stage_label)

            LOG.info(
                "Recovery: Step was successful on disk. Promoting.",
                run_id=run_id,
                from_stage=stage_label,
                to_stage=next_stage,
            )

            with self.lock:
                self.cache.pop(key, None)
                if next_stage != STAGE_TERMINAL_SENTINEL:
                    task_meta.status = ExecutionStatus.PENDING.value
                    new_key = f"{CACHE_TASK_NAMESPACE}:{task_meta.status}:{next_stage}:{identifier}:{run_id}"
                    self.cache[new_key] = task_meta
                else:
                    self.cache.pop(f"active_run:{identifier}", None)
        else:
            # If no symlink exists, the worker died mid-stream or before finalize.
            # Reset to PENDING in the SAME queue to allow a retry.
            LOG.info(
                "Recovery: No physical proof of success. Resetting for retry.",
                run_id=run_id,
                stage=stage_label,
            )

            # Rehydrate the Task to perform a proper manifest reset
            task = Task(
                composite_key=f"{task_meta.job_id}:{task_meta.dataset_id}",
                run_id=task_meta.run_id,
                partition_date=task_meta.partition_date,
                worker_id="engine-recovery",
                exec_ctx=self.exec_ctx,
                target_stage=stage_label,
            )

            task.update_manifest(
                {
                    "status": ExecutionStatus.PENDING.value,
                    "current_stage": None,
                }
            )

            # Sync the engine cache with the new manifest state
            # Clear indicators and set to PENDING
            (task.folder / ".retrying").unlink(missing_ok=True)
            (task.folder / ".blocked").unlink(missing_ok=True)
            with self.lock:
                self.cache.pop(key, None)
                task_meta.status = ExecutionStatus.PENDING.value
                task_meta.last_hb = time.time()

                # Reclaim resources if there was an active Ray worker for this key
                for ref, active_key in list(self._active_tasks.items()):
                    if active_key == key:
                        self.compute.reclaim_resources(ref)
                        self._active_tasks.pop(ref)
                        break

                new_key = f"{CACHE_TASK_NAMESPACE}:{task_meta.status}:{stage_label}:{identifier}:{run_id}"
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
            # Pop from Manager's local tracking[cite: 4]
            self._active_tasks.pop(ref)

            # Tell Compute to free up the budget[cite: 2]
            self.compute.reclaim_resources(ref)
