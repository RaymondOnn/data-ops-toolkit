import os
import subprocess
import time
from typing import Any

import ray
from filelock import FileLock
from loguru import logger
from tenacity import Retrying, stop_after_attempt, wait_exponential

from apps.ingestion.src.core.contexts import ExecutionContext, RayMode
from apps.ingestion.src.core.models.stages.enums import EXEC_STAGES, StageName
from apps.ingestion.src.core.models.task import ExecutionStatus, Task
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.services.registry import ServiceRegistry
from apps.ingestion.src.utils.common import find_path
from apps.ingestion.src.utils.constants import MANIFEST_FILENAME
from apps.ingestion.src.utils.dates import epoch_to_iso, get_end_of_day_ts
from apps.ingestion.src.utils.exceptions import RetryTask, RewindTask
from libs.cache.utils import get_cache
from libs.utils.log import setup_logging

from .enums import TaskMetadata

LOG = logger
IO_POOL_SIZE = 15
CPU_POOL_SIZE = 4


@ray.remote(max_restarts=3, max_task_retries=1)
class Worker:
    def __init__(self, worker_id: str, exec_ctx: ExecutionContext):
        self.worker_id = worker_id
        self.exec_ctx = exec_ctx

        # Initialize logging for the worker process immediately.
        # Reset loguru to clear Ray's inherited handlers and apply local config
        # setup_logging(
        #     log_dir=self.exec_ctx.workspace_dir / "logs",
        #     is_prod=self.exec_ctx.is_prod(),
        #     is_debug=self.exec_ctx.is_debug(),
        # )
        
        # Pass primitives to factory to keep services/ independent of core/
        self.cache = get_cache(self.exec_ctx.workspace_dir, self.exec_ctx.cache_config)

        # Initialize the Secret Provider for this process using the context's config
        ServiceFactory.get_provider(self.exec_ctx.env, self.exec_ctx.provider_config)

        # Share the same cache path with ServiceRegistry so that circuit-breaker
        # state (written by workers) is visible to the Orchestrator's registry.
        ServiceRegistry.configure(
            self.exec_ctx.workspace_dir, self.exec_ctx.cache_config
        )
        self.lock = FileLock(self.exec_ctx.lock_file)


        self.is_busy = False

    def process_stage(self, key: str) -> None:
        run_id = key.rsplit(":", maxsplit=1)[-1]
        current_stage, identifier = key.split(":", 1)
        log = logger.bind(worker_id=self.worker_id, stage=current_stage, run_id=run_id)

        try:
            for attempt in Retrying(
                stop=stop_after_attempt(3),
                wait=wait_exponential(multiplier=1, min=4, max=10),
                reraise=True,
            ):
                with attempt, logger.contextualize(run_id=run_id):
                    # 1. Rehydrate Task
                    with self.lock:
                        # Update status to RUNNING immediately so Engine occupancy tracking is accurate
                        meta: TaskMetadata = self.cache.get(key)
                        meta.status = ExecutionStatus.RUNNING.value
                        meta.last_hb = time.time()
                        self.cache[key] = meta

                        LOG.info(
                            "Updating job status",
                            job_id=meta.job_id,
                            run_id=meta.run_id,
                            status=meta.status,
                        )

                    task: Task = Task(
                        composite_key=f"{meta.job_id}:{meta.dataset_id}",
                        run_id=meta.run_id,
                        partition_date=meta.partition_date,
                        worker_id=self.worker_id,
                        exec_ctx=self.exec_ctx,
                        target_stage=current_stage,
                    )
                    
                    setup_logging(
                        log_dir=self.exec_ctx.workspace_dir / "logs",
                        is_prod=self.exec_ctx.is_prod(),
                        is_debug=self.exec_ctx.is_debug(),
                        # Name the log by stage for easy multi-stage debugging
                        filename=f"{task.id}.jsonl",
                    )

                    # Cleanup indicators when starting execution
                    (task.folder / ".retrying").unlink(missing_ok=True)
                    (task.folder / ".blocked").unlink(missing_ok=True)

                    self.is_busy = True
                    log.info(
                        "Worker started processing {current_stage} stage",
                        run_id=meta.run_id,
                        job_id=meta.job_id,
                        current_stage=current_stage,
                    )

                    # 1. OPTION A: Isolated Execution via PEX (Production Mode)
                    if (
                        self.exec_ctx.code_pex_path
                        and self.exec_ctx.code_pex_path.exists()
                    ):
                        log.info(
                            "Launching isolated PEX process",
                            pex=str(self.exec_ctx.code_pex_path),
                        )
                        env = os.environ.copy()
                        if self.exec_ctx.deps_pex_path:
                            env["PEX_PATH"] = str(self.exec_ctx.deps_pex_path)

                        cmd = [
                            "python3",
                            str(self.exec_ctx.code_pex_path),
                            "run",
                            meta.partition_date,
                            "--job-id",
                            meta.job_id,
                            "--dataset",
                            meta.dataset_id,
                            "--stage",
                            current_stage,
                        ]
                        subprocess.run(cmd, env=env, check=True)
                    else:
                        # 2. OPTION B: Direct Import Execution (Dev/Fallback Mode)
                        task.check_in(current_stage)
                        task.stage.pre_flight(task)
                        next_stage = task.execute()

                    # 4. Success Handoff
                    with self.lock:
                        # Every execution ends the "Current" stage lease.
                        # We pop it regardless of what comes next.
                        self.cache.pop(key, None)

                        if next_stage:
                            if next_stage.casefold() in EXEC_STAGES:
                                # Progressing to next queue
                                meta.current_stage = next_stage
                                meta.status = ExecutionStatus.PENDING.value
                                self.cache[f"{next_stage}:{identifier}"] = meta
                                log.info(
                                    "'{current_stage}' Step complete. "
                                    "Progressing to {next_stage}...",
                                    current_stage=current_stage,
                                    next_stage=next_stage,
                                )
                            else:
                                # Task fully reached SUCCESS/FINISH
                                log.info("Task fully completed.")
                                self.cache.pop(f"active_run:{identifier}", None)
        except RetryTask as r:
            log.warning(
                "Task signaled RETRY (transient outage)",
                reason=r.reason,
                wait=r.wait_seconds,
            )
            with self.lock:
                meta: TaskMetadata = self.cache.get(key)
                if meta:
                    # We use last_hb as the 'earliest resume time'
                    # This prevents the orchestrator from picking it up too soon
                    if hasattr(r, "source_name") and r.source_name:
                        # Case: Waiting for .source_down signal
                        meta.status = ExecutionStatus.BLOCKED
                        meta.blocked_by = r.source_name
                    else:
                        # Case: Normal scheduled retry
                        meta.status = ExecutionStatus.RETRY
                    meta.last_hb = time.time() + r.wait_seconds
                    self.cache[key] = meta
            return  # Yield Ray Actor
        except RewindTask as rw:
            log.warning(
                "Task signaled REWIND (Self-Healing)",
                to_stage=rw.target_stage,
                reason=rw.reason,
            )
            with self.lock:
                # 1. Clear the current lease
                self.cache.pop(key, None)

                # 2. Reset the manifest on disk for the target stage
                # (We clear the payload and set status to PENDING)
                task.update_manifest(
                    {
                        "status": ExecutionStatus.PENDING.value,
                        rw.target_stage: None,
                        "current_stage": rw.target_stage,
                    }
                )

                # 3. Put back into the specific target queue
                meta.current_stage = rw.target_stage
                meta.status = ExecutionStatus.PENDING.value
                self.cache[f"{rw.target_stage}:{identifier}"] = meta
        except Exception:
            log.exception("Task stage reached terminal failure/hold")
            with self.lock:
                # For terminal failures, we remove the key from the active cache.
                # The folder structure (HOLD/FAILED) becomes the source of truth.
                self.cache.pop(key, None)
                # We keep 'active_run' key so the Orchestrator doesn't
                # re-trigger the same partition while a failed run is present.
            raise
        finally:
            self.is_busy = False

    def _handle_handoff(self, key, identifier, current_stage, next_stage, meta, log):
        """Encapsulated logic for moving tasks between stage queues."""
        with self.lock:
            # Every execution ends the "Current" stage lease.
            self.cache.pop(key, None)

            if next_stage:
                if next_stage.casefold() in EXEC_STAGES:
                    # Progressing (or Rewinding) to specific queue
                    meta.current_stage = next_stage
                    meta.status = ExecutionStatus.PENDING.value
                    # We reset last_hb so it's eligible for immediate pickup if it's a rewind
                    meta.last_hb = time.time()
                    self.cache[f"{next_stage}:{identifier}"] = meta
                    log.info(
                        "Transitioning: {current_stage} -> {next_stage}",
                        current_stage=current_stage,
                        next_stage=next_stage,
                    )
                else:
                    # Task fully reached SUCCESS/FINISH
                    log.info("Task fully completed.")
                    self.cache.pop(f"active_run:{identifier}", None)

    def is_idle(self) -> bool:
        return not self.is_busy


class TaskManger:
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
        self.lock = FileLock(self.exec_ctx.lock_file)

        if not ray.is_initialized():
            local_mode = self.exec_ctx.ray_mode == RayMode.LOCAL
            ray.init(ignore_reinit_error=True, local_mode=local_mode)

        self.is_degraded = False
        # Configuration for stage limits
        self.stage_limits: dict[StageName, dict[str, Any]] = {
            StageName.START: {"limit": 5, "pool": "io"},
            StageName.EXTRACT: {"limit": 10, "pool": "io"},
            StageName.TRANSFORM: {"limit": 4, "pool": "cpu"},  # CPU-Heavy
            StageName.WRITE: {"limit": 5, "pool": "io"},
            # StageName.AUDIT: {"limit": 4, "pool": "io"},
            StageName.PUBLISH: {
                "limit": 1,
                "pool": "io",
            },  # Sequential promotion
            StageName.COMPLETE: {"limit": 5, "pool": "io"},
        }

        try:
            self.exec_ctx.check_serializability()
            # Initialize specialized pools and track worker IDs locally for logging
            self.worker_map: dict[ray.actor.ActorHandle, str] = {}
            self.io_pool: list[ray.actor.ActorHandle] = []
            self.cpu_pool: list[ray.actor.ActorHandle] = []

            for i in range(IO_POOL_SIZE):
                w_id = f"io_{i}"
                handle = Worker.remote(w_id, self.exec_ctx)
                self.io_pool.append(handle)
                self.worker_map[handle] = w_id

            for i in range(CPU_POOL_SIZE):
                w_id = f"cpu_{i}"
                handle = Worker.remote(w_id, self.exec_ctx)
                self.cpu_pool.append(handle)
                self.worker_map[handle] = w_id
        except Exception as e:
            # If the context is not serializable, we cannot proceed with Ray workers.
            # Log the error and raise an exception to prevent silent failures.
            LOG.error(
                "Failed to initialize Ray workers due to unserializable context",
                error=str(e),
            )
            raise TypeError(f"ExecutionContext is not serializable: {e}") from e
        # Local tracker for active Ray tasks to avoid blocking RPC calls
        self._active_tasks: dict[ray.ObjectRef, ray.actor.ActorHandle] = {}

    def run(self) -> None:
        """Main loop managing multiple tasks."""
        LOG.info("Starting Orchestrator...")
        while True:
            self.scan_and_recover()
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
        queue_key = f"{current_stage}:{identifier}:{run_id}"
        job_id, dataset_id, partition_date = identifier.split(":")

        # ?: Logic to determine if this is a snapshot (e.g., based on
        # job naming convention)
        is_snapshot = "snapshot" in job_id.lower()
        expires_at = get_end_of_day_ts() if is_snapshot else None

        # Log the dispatch with human-readable timestamps
        expiry_str = epoch_to_iso(expires_at) if expires_at else "NEVER"

        with self.lock:
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
        # 1. Calculate current occupancy for all stages
        current_occupancy = self._get_current_occupancy()

        # 2. Snapshot pool availability to exit early if entire pools are full
        pool_status = {
            "io": self._get_idle_worker_from_pool(self.io_pool) is not None,
            "cpu": self._get_idle_worker_from_pool(self.cpu_pool) is not None,
        }

        # Take a snapshot of keys starting with valid stages to iterate without holding the lock
        for key in self._get_all_keys():
            # If both pools are full, stop processing immediately
            if not any(pool_status.values()):
                LOG.debug("Both pools are full, stopping processing")
                break

            # Convert the string label from the cache key to a ExecutionStage enum
            stage_label = key.split(":", 1)[0]
            stage_enum = StageName.from_label(stage_label)
            if stage_enum not in self.stage_limits:
                continue

            limit = self.stage_limits[stage_enum]["limit"]
            pool_type = self.stage_limits[stage_enum]["pool"]

            # Respect both the stage-specific "dedicated" limit and pool availability
            if current_occupancy[stage_enum] >= limit or not pool_status[pool_type]:
                LOG.debug(
                    "Stage limit reached or pool exhausted, skipping",
                    stage=stage_enum,
                    occupancy=current_occupancy[stage_enum],
                    limit=limit,
                    pool_status=pool_status[pool_type],
                )
                continue

            # Only lock when we have a potential candidate to update
            with self.lock:
                task_meta: TaskMetadata = self.cache.get(key)

                # # A task is ready if it's PENDING, or if it's in RETRY and the wait is over
                is_pending = task_meta.status == ExecutionStatus.PENDING.value
                # Case 1: Normal Retry (Timer based)
                is_retry_ready = (
                    task_meta.status == "RETRY" and time.time() >= task_meta.last_hb
                )
                # Case 2: Service Outage (Signal based)
                is_blocked_ready = (
                    task_meta.status == "BLOCKED"
                    and ServiceRegistry.is_healthy(task_meta.blocked_by or "")
                    and time.time() >= task_meta.last_hb
                )

                if not (is_pending or is_retry_ready or is_blocked_ready):
                    LOG.debug(
                        "Task {run_id} not pending or retry-ready, skipping",
                        job_id=task_meta.job_id,
                        run_id=task_meta.run_id,
                        status=task_meta.status,
                    )
                    continue

                if task_meta.status == ExecutionStatus.PENDING.value:
                    # if is_pending or is_retry_ready:
                    # --- BACKPRESSURE LOGIC ---
                    # If the system is degraded (high memory), we pause new EXTRACTIONs
                    # to allow the LOAD/WRITE stages to clear the disk/memory backlog.
                    if self.is_degraded and stage_enum == StageName.EXTRACT:
                        LOG.warning(
                            "System DEGRADED: Pausing EXTRACT task",
                            job_id=task_meta.job_id,
                        )
                        continue

                    # 2. Select the correct Worker Pool
                    # 3. Find a free Ray worker
                    pool = (
                        self.cpu_pool
                        if self.stage_limits[stage_enum]["pool"] == "cpu"
                        else self.io_pool
                    )
                    if worker := self._get_idle_worker_from_pool(pool):
                        LOG.success(
                            "Dispatching task to worker",
                            job_id=task_meta.job_id,
                            dataset_id=task_meta.dataset_id,
                            partition_date=task_meta.partition_date,
                            run_id=task_meta.run_id,
                            stage=stage_enum,
                            worker_id=self.worker_map.get(worker, "unknown"),
                        )
                        task_meta.status = ExecutionStatus.PROVISIONING.value
                        task_meta.last_hb = time.time()
                        self.cache[key] = task_meta
                        LOG.info(
                            "Updating task status",
                            job_id=task_meta.job_id,
                            status=task_meta.status,
                        )

                        # Update local occupancy count
                        current_occupancy[stage_enum] += 1

                        # Dispatch non-blocking and track the reference
                        ref = worker.process_stage.remote(key)
                        self._active_tasks[ref] = worker
                    else:
                        # Pool is exhausted for this tick; mark it so we stop checking related stages
                        pool_status[pool_type] = False

    def _get_current_occupancy(self) -> dict[StageName, int]:
        """Counts how many workers are active in each stage."""
        counts = dict.fromkeys(self.stage_limits, 0)
        for stage_enum in self.stage_limits:
            # Only iterate over keys for specific stages to avoid full cache scans
            for key in self.cache.iterkeys(pattern=f"{stage_enum.label}:*"):
                meta = self.cache.get(key)
                if (
                    isinstance(meta, TaskMetadata)
                    and meta.status in ExecutionStatus.dispatched_statuses()
                ):
                    counts[stage_enum] += 1
        return counts

    def _get_all_keys(self) -> list[str]:
        """Fetches a snapshot of all keys from the cache that correspond to valid execution stages."""
        all_keys = []
        for stage_label in EXEC_STAGES:
            all_keys.extend(self.cache.iterkeys(pattern=f"{stage_label}:*"))
        return all_keys

    def _get_idle_worker_from_pool(
        self, pool: list[ray.actor.ActorHandle]
    ) -> Any | None:
        # 1. Clean up finished tasks from our tracker without blocking
        if self._active_tasks:
            ready, _ = ray.wait(
                list(self._active_tasks.keys()),
                timeout=0,
                num_returns=len(self._active_tasks),
            )
            for ref in ready:
                worker = self._active_tasks.get(ref)
                self._active_tasks.pop(ref, None)
                worker_id = (
                    self.worker_map.get(worker, "unknown") if worker else "unknown"
                )
                try:
                    # Retrieve the result to re-raise any remote exceptions
                    # into the Orchestrator's context.
                    ray.get(ref)
                except Exception as e:
                    # RayTaskError includes the remote traceback automatically
                    LOG.error(
                        "Ray worker task failed",
                        worker_id=worker_id,
                        error_type=type(e).__name__,
                        error_msg=str(e),
                    )

        # 2. Find a worker that isn't currently assigned a task
        busy_workers = set(self._active_tasks.values())
        for worker in pool:
            if worker not in busy_workers:
                # LOG.debug("Found idle worker", worker=worker)
                return worker

        LOG.debug("No idle workers found")
        return None

    # TODO: Handle Timeouts
    def scan_and_recover(self) -> None:
        """Scans all stage queues for zombie tasks."""
        with self.lock:
            # Iterate only over keys starting with valid stages
            for key in self._get_all_keys():
                key_str = key if isinstance(key, str) else key.decode("utf-8")
                stage_label, rest = key_str.split(":", 1)

                meta: TaskMetadata = self.cache.get(key)  # type: ignore

                # --- ENHANCED HEARTBEAT LOGIC ---
                # We check for zombies in any 'dispatched' state (PROVISIONING or RUNNING).
                # This ensures we recover from Ray actor startup failures as well.
                if (
                    meta.status in ExecutionStatus.dispatched_statuses()
                    and time.time() - meta.last_hb > 300
                ):
                    # Check the 'Physical Heartbeat' (Manifest timestamp)
                    # before declaring it a zombie.
                    active_path = find_path(self.exec_ctx.active_path, meta.run_id)
                    manifest_file = (
                        active_path / MANIFEST_FILENAME if active_path else None
                    )

                    if manifest_file and manifest_file.exists():
                        mtime = manifest_file.stat().st_mtime
                        if time.time() - mtime < 300:
                            # Physical heartbeat is fresh! Update cache and move on.
                            meta.last_hb = time.time()
                            self.cache[key] = meta
                            continue

                    LOG.warning(
                        "Zombie task detected - no activity on cache or disk",
                        key=key_str,
                    )
                    self._recover_task(stage_label, rest)

    def _recover_task(self, stage_name: str, rest_of_key: str) -> None:
        """
        Recovers a stalled task by checking its physical progress.
        """
        with self.lock:
            key = f"{stage_name}:{rest_of_key}"

            # .get() returns Optional[Any], so we check type and None-ness at once
            task_meta = self.cache.get(key)
            if not isinstance(task_meta, TaskMetadata):
                return

        job_id = task_meta.job_id
        run_id = task_meta.run_id

        # 1. Verify if the stage actually finished on disk but failed to transit
        # We check for the .success marker in the current stage's folder
        if self._check_stage_completion_on_disk(run_id, stage_name):
            # Promotion: Find the next stage using the StageName domain model
            next_stage_obj = StageName.next(stage_name)
            next_stage = next_stage_obj.label if next_stage_obj else "complete"

            LOG.info(
                "Recovery: Step was successful on disk. Promoting.",
                job_id=job_id,
                from_stage=stage_name,
                to_stage=next_stage,
            )

            self.cache.delete(key)
            if next_stage.casefold() != EXEC_STAGES[-1].casefold():
                task_meta.status = ExecutionStatus.PENDING.value
                self.cache[f"{next_stage}:{rest_of_key}"] = task_meta
        else:
            # If no symlink exists, the worker died mid-stream or before finalize.
            # Reset to PENDING in the SAME queue to allow a retry.
            LOG.info(
                "Recovery: No physical proof of success. Resetting for retry.",
                job_id=job_id,
                stage=stage_name,
            )

            # Rehydrate the Task to perform a proper manifest reset
            task = Task(
                composite_key=f"{task_meta.job_id}:{task_meta.dataset_id}",
                run_id=task_meta.run_id,
                partition_date=task_meta.partition_date,
                worker_id="engine-recovery",
                exec_ctx=self.exec_ctx,
                target_stage=stage_name,
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

            task_meta.status = ExecutionStatus.PENDING.value
            task_meta.last_hb = time.time()
            self.cache[key] = task_meta

    def _check_stage_completion_on_disk(self, run_id: str, stage_name: str) -> bool:
        """
        Checks if the 'active' symlink for this stage exists.
        This is the definitive proof of success in our new structure.
        """
        # Find active path
        # Logic: active/{job_id}:{dataset_id}_{partition_date}/run_id/stage_name
        active_root = self.exec_ctx.active_path

        active_path = find_path(active_root, run_id)
        active_path = active_path / stage_name

        # It must exist and be a valid link/directory
        return bool(active_path.exists())

    # def _recover_from_manifest(self, job_id: str, run_id: str) -> str:
    #     """
    #     Decision: Single-File Peep.
    #     Peeps at the manifests on disk to find where the job stalled.
    #     Used during System Recovery or Orchestrator Boot-up to decide exactly
    #     where to resume a job that was interrupted.
    #     Instead of searching 5+ folders, we read the one 'active' manifest.
    #     This is O(1) instead of O(N).
    #     """

    #     active_root = self.exec_ctx.active_path
    #     active_path = find_path(active_root, run_id)
    #     manifest_path = active_path / MANIFEST_FILENAME

    #     if not manifest_path.exists():
    #         LOG.info("No active manifest found, starting fresh.", job_id=job_id)
    #         return str(EXEC_STAGES[0])  # Usually 'start'

    #     with manifest_path.open("rb") as f:
    #         # msgspec is fast enough to do this in the main recovery loop
    #         meta = msgspec.json.decode(f.read(), type=TaskManifest)

    #         # Logic: If the current stage is done, move forward.
    #         # Otherwise, the stage crashed mid-way; resume/retry it.
    #         for stage in EXEC_STAGES:
    #             if hasattr(meta, stage):
    #                 continue

    #             return stage

    #     return "FINISH"

    # def get_latest_manifest(self, job_id: str, run_id: str) -> TaskManifest:
    #     """
    #     Decision: Direct Access.
    #     """
    #     active_root = self.exec_ctx.active_path
    #     active_path = find_path(active_root, run_id)
    #     manifest_path = active_path / MANIFEST_FILENAME

    #     if not manifest_path.exists():
    #         # During recovery, if a job exists in DB but not on disk, it's a 'Ghost'
    #         raise FileNotFoundError(f"Active workspace missing for {job_id}")

    #     with manifest_path.open("rb") as f:
    #         # Structural validation included via msgspec
    #         return msgspec.json.decode(f.read(), type=TaskManifest)
