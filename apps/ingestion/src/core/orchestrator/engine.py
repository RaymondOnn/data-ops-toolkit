import os
import subprocess
import time
from typing import Any

import diskcache
import msgspec
import ray
import structlog
from filelock import FileLock

from apps.ingestion.src.core.contexts import ExecutionContext, RayMode
from apps.ingestion.src.core.models.job import ExecutionStatus, Task, TaskManifest
from apps.ingestion.src.core.models.stages.enums import EXEC_STAGES, StageName
from apps.ingestion.src.services.factory import ServiceFactory
from apps.ingestion.src.services.registry import ServiceRegistry
from apps.ingestion.src.utils.common import find_path
from apps.ingestion.src.utils.constants import DISKCACHE_FILE_PATH, MANIFEST_FILENAME
from apps.ingestion.src.utils.dates import epoch_to_iso, get_end_of_day_ts
from libs.utils.log import setup_logging

from .enums import TaskMetadata

LOG = structlog.getLogger(__name__)
IO_POOL_SIZE = 15
CPU_POOL_SIZE = 4


@ray.remote
class Worker:
    def __init__(self, worker_id: str, exec_ctx: ExecutionContext):
        self.worker_id = worker_id
        self.exec_ctx = exec_ctx
        self.cache = diskcache.Cache(
            (self.exec_ctx.workspace_dir / DISKCACHE_FILE_PATH).resolve(),
            timeout=10,  # Increase timeout for slow PV file locks (NFS/EFS)
            disk_pickle_protocol=4,  # Recommended for Python 3.8+
            # Unpack settings or just pass them as kwargs directly
            sqlite_journal_mode="wal",
            sqlite_synchronous=1,  # 'NORMAL' - better for WAL mode performance
        )

        # Initialize the Secret Provider for this process using the context's config
        if getattr(self.exec_ctx, "provider_config", None):
            ServiceFactory.get_provider(
                self.exec_ctx.env, self.exec_ctx.provider_config
            )

        # Share the same cache path with ServiceRegistry so that circuit-breaker
        # state (written by workers) is visible to the Orchestrator's registry.
        ServiceRegistry.configure(self.exec_ctx.workspace_dir)
        self.lock = FileLock(self.exec_ctx.lock_file)

        # Initialize logging for the worker process.
        # Using 'platform.jsonl' for workers as they are shared across runs.
        log_dir = self.exec_ctx.workspace_dir / "logs"
        setup_logging(log_dir=log_dir, is_prod=self.exec_ctx.is_prod())
        self.is_busy = False

    def process_stage(self, key: str) -> None:
        current_stage, _ = key.split(":", 1)
        log = structlog.get_logger().bind(worker_id=self.worker_id, stage=current_stage)

        # 1. Rehydrate Task
        with self.lock:
            raw_meta = self.cache[key]
            if not isinstance(raw_meta, TaskMetadata):
                raise ValueError(f"Invalid task metadata for key {key}: {raw_meta}")
            meta: TaskMetadata = raw_meta

        # Update status to RUNNING immediately so Engine occupancy tracking is accurate
        with self.lock:
            meta.status = ExecutionStatus.RUNNING.value
            self.cache[key] = meta

        task: Task = Task(
            composite_key=f"{meta.job_id}:{meta.dataset_id}",
            run_id=meta.run_id,
            run_date=meta.run_date,
            worker_id=self.worker_id,
            exec_ctx=self.exec_ctx,
            target_stage=current_stage,
        )
        self.is_busy = True
        log.info(
            "Worker started processing stage", run_id=meta.run_id, job_id=meta.job_id
        )

        # Ensure the job's workspace is fully provisioned before check-in
        _ = task.folder

        # 1. OPTION A: Isolated Execution via PEX (Production Mode)
        if self.exec_ctx.code_pex_path and self.exec_ctx.code_pex_path.exists():
            log.info(
                "Launching isolated PEX process", pex=str(self.exec_ctx.code_pex_path)
            )
            try:
                env = os.environ.copy()
                if self.exec_ctx.deps_pex_path:
                    env["PEX_PATH"] = str(self.exec_ctx.deps_pex_path)

                cmd = [
                    "python3",
                    str(self.exec_ctx.code_pex_path),
                    "run",
                    meta.run_date,
                    "--job-id",
                    meta.job_id,
                    "--dataset",
                    meta.dataset_id,
                    "--stage",
                    current_stage,
                ]

                # This blocks the Ray Actor until the PEX finishes
                subprocess.run(cmd, env=env, check=True)

                # If successful, we assume the PEX updated the manifest and handled finalization
                # We return here to avoid executing Option B
                return
            except subprocess.CalledProcessError as e:
                log.error("PEX process failed", exit_code=e.returncode)
                # Fall through to the except block to mark the task as FAILED in cache
                raise e

        try:
            # 2. OPTION B: Direct Import Execution (Dev/Fallback Mode)
            task.check_in(current_stage)

            # 2.5 Pre-flight validation (Resource & Connectivity check)
            task.stage.pre_flight(task)

            # 3. Execute and get the next stage signal (returns label or 'FINISH')
            next_stage = task.execute()

            # 4. Atomic Handoff
            identifier = self.exec_ctx.get_task_identifier(
                job_id=meta.job_id,
                dataset_id=meta.dataset_id,
                run_date=meta.run_date,
            )

            with self.lock:
                # Remove from current queue
                self.cache.pop(key)

                # Push to next queue if not finished
                if not next_stage:
                    log.warning("Step returned no signal", next_stage=next_stage)
                    return

                if next_stage.casefold() in EXEC_STAGES:
                    meta.current_stage = next_stage
                    meta.status = (
                        ExecutionStatus.PENDING.value
                    )  # Ready for the next worker pool
                    new_key = f"{next_stage}:{identifier}:{meta.run_id}"
                    self.cache[new_key] = meta
                    log.info(
                        "Step complete. Task returned to queue.",
                        next_stage=next_stage,
                    )
                else:
                    log.info("Task fully completed.")
                    # Cleanup lookup key used by Orchestrator
                    lookup_key = f"active_run:{identifier}"
                    self.cache.pop(lookup_key, None)

        except Exception as e:
            log.error("Task stage failed terminaly", error=str(e))
            with self.lock:
                meta.status = ExecutionStatus.FAILED.value
                meta.last_hb = time.time()
                self.cache[key] = meta
            # We do NOT re-raise. The Orchestrator will see the 'FAILED'
            # status in the cache.
        finally:
            self.is_busy = False

    def is_idle(self) -> bool:
        return not self.is_busy


class IngestionEngine:
    def __init__(self, exec_ctx: ExecutionContext, cache_dir: str = ".cache/ingestion"):
        self.exec_ctx = exec_ctx
        # 2. Initialize the Global Registry (Diskcache)
        # This ensures the shared cache path exists for all Ray workers
        ServiceRegistry.configure(self.exec_ctx.workspace_dir)
        self.registry = ServiceRegistry()
        cache_path = (self.exec_ctx.workspace_dir / DISKCACHE_FILE_PATH).resolve()

        self.cache = diskcache.Cache(cache_path)
        self.lock = FileLock(self.exec_ctx.lock_file)

        if not ray.is_initialized():
            local_mode = self.exec_ctx.ray_mode == RayMode.LOCAL
            ray.init(ignore_reinit_error=True, local_mode=local_mode)

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

        # Initialize specialized pools
        self.io_pool: list[ray.actor.ActorHandle] = [
            Worker.remote(f"io_{i}", self.exec_ctx) for i in range(IO_POOL_SIZE)  # type: ignore
        ]
        self.cpu_pool: list[ray.actor.ActorHandle] = [
            Worker.remote(f"cpu_{i}", self.exec_ctx) for i in range(CPU_POOL_SIZE)  # type: ignore
        ]

        # Local tracker for active Ray tasks to avoid blocking RPC calls
        self._active_tasks: dict[ray.ObjectRef, ray.actor.ActorHandle] = {}

    def run(self) -> None:
        """Main loop managing multiple tasks."""
        LOG.info("Starting Orchestrator...")
        while True:
            self.scan_and_recover()
            self._process_jobs()
            time.sleep(10)  # Frequency of polling

    def queue_jobs(
        self,
        identifier: str,
        run_id: str,
        config_file_path: str,
        current_stage: str | None = None,
    ) -> None:
        """Checks config, creates jobs if not in cache, and submits them."""
        # Always start in the 'start' queue
        current_stage = current_stage or "start"
        queue_key = f"{current_stage}:{identifier}:{run_id}"
        job_id, dataset_id, run_date = identifier.split(":")

        # Logic to determine if this is a snapshot (e.g., based on
        # job naming convention)
        is_snapshot = "snapshot" in job_id.lower()
        expires_at = get_end_of_day_ts() if is_snapshot else None

        # Log the dispatch with human-readable timestamps
        expiry_str = epoch_to_iso(expires_at) if expires_at else "NEVER"

        with self.lock:
            # Check if job exists in cache
            if queue_key not in self.cache:
                # Brand new entry
                meta = TaskMetadata(
                    job_id=job_id,
                    run_id=run_id,
                    dataset_id=dataset_id,
                    run_date=run_date,
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

    def _process_jobs(self) -> None:
        # 1. Calculate current occupancy per stage
        # We count how many keys per prefix have status 'RUNNING'
        current_occupancy = self._get_current_occupancy()

        with self.lock:
            # We take a snapshot of keys to avoid 'dict changed size' during iteration
            for key in list(self.cache.iterkeys()):
                if not isinstance(key, str) or ":" not in key:
                    continue

                # Convert the string label from the cache key to a ExecutionStage enum
                stage_label = key.split(":", 1)[0]
                try:
                    stage_enum = StageName[stage_label.upper()]
                except (KeyError, ValueError):
                    continue

                if stage_enum not in self.stage_limits:
                    continue

                raw_meta = self.cache[key]
                if not isinstance(raw_meta, TaskMetadata):
                    continue
                task_meta: TaskMetadata = raw_meta

                if task_meta.status == ExecutionStatus.PENDING.value:
                    limit = self.stage_limits[stage_enum]["limit"]
                    # 2. Check if the specific stage has room
                    if current_occupancy[stage_enum] < limit:
                        # 2. Select the correct Worker Pool
                        # 3. Find a free Ray worker
                        pool = (
                            self.cpu_pool
                            if self.stage_limits[stage_enum]["pool"] == "cpu"
                            else self.io_pool
                        )
                        if worker := self._get_idle_worker_from_pool(pool):
                            task_meta.status = ExecutionStatus.PROVISIONING.value
                            task_meta.last_hb = time.time()
                            self.cache[key] = task_meta

                            # Update local occupancy count
                            current_occupancy[stage_enum] += 1

                            # Dispatch non-blocking and track the reference
                            ref = worker.process_stage.remote(key)
                            self._active_tasks[ref] = worker

    def _get_current_occupancy(self) -> dict[StageName, int]:
        """Counts how many workers are active in each stage."""
        counts = dict.fromkeys(self.stage_limits, 0)
        for key in list(self.cache.iterkeys()):
            if isinstance(key, str) and ":" in key:
                stage_label = key.split(":", 1)[0]
                try:
                    stage_enum = StageName[stage_label.upper()]
                except (KeyError, ValueError):
                    continue

                if stage_enum not in self.stage_limits:
                    continue

                meta = self.cache[key]
                if (
                    isinstance(meta, TaskMetadata)
                    and meta.status == ExecutionStatus.RUNNING.value
                ):
                    counts[stage_enum] += 1
        return counts

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
                try:
                    # Retrieve the result to re-raise any remote exceptions
                    # into the Orchestrator's context.
                    ray.get(ref)
                except Exception as e:
                    LOG.error("Ray worker task failed", error=str(e))
                self._active_tasks.pop(ref, None)

        # 2. Find a worker that isn't currently assigned a task
        busy_workers = set(self._active_tasks.values())
        for worker in pool:
            if worker not in busy_workers:
                return worker
        return None

    # TODO: Handle Timeouts
    def scan_and_recover(self) -> None:
        """Scans all stage queues for zombie jobs."""
        with self.lock:
            for key in list(self.cache.iterkeys()):
                key_str = key if isinstance(key, str) else key.decode("utf-8")
                if ":" not in key_str:
                    continue

                stage_label, rest = key_str.split(":", 1)
                try:
                    stage_enum = StageName[stage_label.upper()]
                except (KeyError, ValueError):
                    continue

                if stage_enum not in self.stage_limits:
                    continue

                raw_meta = self.cache[key]
                if not isinstance(raw_meta, TaskMetadata):
                    continue

                meta: TaskMetadata = raw_meta

                # --- ENHANCED HEARTBEAT LOGIC ---
                if (
                    meta.status == ExecutionStatus.RUNNING.value
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
                    self._recover_job(stage_label, rest)

    def _recover_job(self, stage_name: str, rest_of_key: str) -> None:
        """
        Recovers a stalled job by checking its physical progress.
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
            # If the symlink exists, the worker finished 'finalize' but the engine died
            next_stage = self._get_next_stage_name(stage_name)
            LOG.info(
                "Recovery: Step was successful on disk. Promoting.",
                job_id=job_id,
                from_stage=stage_name,
                to_stage=next_stage,
            )

            del self.cache[key]
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
            task_meta.status = ExecutionStatus.PENDING.value
            task_meta.last_hb = time.time()
            self.cache[key] = task_meta

    def _check_stage_completion_on_disk(self, run_id: str, stage_name: str) -> bool:
        """
        Checks if the 'active' symlink for this stage exists.
        This is the definitive proof of success in our new structure.
        """
        # Find active path
        # Logic: active/{job_id}:{dataset_id}_{run_date}/run_id/stage_name
        active_root = self.exec_ctx.active_path

        active_path = find_path(active_root, run_id)
        active_path = active_path / stage_name

        # It must exist and be a valid link/directory
        return bool(active_path.exists())

    def _get_next_stage_name(self, current_stage: str) -> str:
        """
        Decision: Use List-Index Lookup.
        Leveraging a list makes the pipeline order explicit and easy to change.
        """

        try:
            current_idx = EXEC_STAGES.index(current_stage)
            # Return next stage, or 'complete' if we are at the end
            if current_idx + 1 < len(EXEC_STAGES):
                return str(EXEC_STAGES[current_idx + 1])
            return "complete"
        except ValueError:
            # If the stage is unknown (e.g., job just started), start at the beginning
            return str(EXEC_STAGES[0])

    def _recover_from_manifest(self, job_id: str, run_id: str) -> str:
        """
        Decision: Single-File Peep.
        Peeps at the manifests on disk to find where the job stalled.
        Used during System Recovery or Orchestrator Boot-up to decide exactly
        where to resume a job that was interrupted.
        Instead of searching 5+ folders, we read the one 'active' manifest.
        This is O(1) instead of O(N).
        """

        active_root = self.exec_ctx.active_path
        active_path = find_path(active_root, run_id)
        manifest_path = active_path / MANIFEST_FILENAME

        if not manifest_path.exists():
            LOG.info("No active manifest found, starting fresh.", job_id=job_id)
            return str(EXEC_STAGES[0])  # Usually 'start'

        with manifest_path.open("rb") as f:
            # msgspec is fast enough to do this in the main recovery loop
            meta = msgspec.json.decode(f.read(), type=TaskManifest)

            # Logic: If the current stage is done, move forward.
            # Otherwise, the stage crashed mid-way; resume/retry it.
            for stage in EXEC_STAGES:
                if hasattr(meta, stage):
                    continue

                return stage

        return "FINISH"

    def get_latest_manifest(self, job_id: str, run_id: str) -> TaskManifest:
        """
        Decision: Direct Access.
        """
        active_root = self.exec_ctx.active_path
        active_path = find_path(active_root, run_id)
        manifest_path = active_path / MANIFEST_FILENAME

        if not manifest_path.exists():
            # During recovery, if a job exists in DB but not on disk, it's a 'Ghost'
            raise FileNotFoundError(f"Active workspace missing for {job_id}")

        with manifest_path.open("rb") as f:
            # Structural validation included via msgspec
            return msgspec.json.decode(f.read(), type=TaskManifest)
