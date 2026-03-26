import time
from pathlib import Path
from typing import Any

import diskcache
import msgspec
import ray
import structlog
from filelock import FileLock

from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.models.job import Job, JobManifest, JobStatus
from apps.ingestion.src.core.models.steps.enums import STEP_ORDER
from apps.ingestion.src.services.registry import ServiceRegistry
from apps.ingestion.src.utils.common import find_path
from apps.ingestion.src.utils.constants import DISKCACHE_FILE_PATH
from apps.ingestion.src.utils.dates import epoch_to_iso, get_end_of_day_ts
from libs.utils.log import setup_logging

from .enums import JobMetadata

LOG = structlog.getLogger(__name__)
CACHE_DIR = ".cache/ingestion"


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
        # Share the same cache path with ServiceRegistry so that circuit-breaker
        # state (written by workers) is visible to the Orchestrator's registry.
        ServiceRegistry.configure(self.exec_ctx.workspace_dir)
        self.lock = FileLock(self.exec_ctx.workspace_dir / "orchestrator.lock")

        # Initialize logging for the worker process.
        # Using 'platform.jsonl' for workers as they are shared across runs.
        log_dir = self.exec_ctx.workspace_dir / "logs"
        setup_logging(log_dir=log_dir, is_prod=not self.exec_ctx.is_debug())
        self.is_busy = False

    def process_step(self, key: str, config_file_path: str | Path) -> None:
        current_step, _ = key.split(":", 1)
        log = structlog.get_logger().bind(worker_id=self.worker_id, step=current_step)

        # 1. Rehydrate Job
        with self.lock:
            raw_meta = self.cache[key]
            if not isinstance(raw_meta, JobMetadata):
                raise ValueError(f"Invalid job metadata for key {key}: {raw_meta}")
            meta: JobMetadata = raw_meta

        # Update status to RUNNING immediately so Engine occupancy tracking is accurate
        with self.lock:
            meta.status = JobStatus.RUNNING.value
            self.cache[key] = meta

        job: Job = Job(
            composite_key=f"{meta.job_id}:{meta.dataset_id}",
            run_id=meta.run_id,
            run_date=meta.run_date,
            worker_id=self.worker_id,
            exec_ctx=self.exec_ctx,
            target_step=current_step,
        )
        self.is_busy = True
        log.info(
            "Worker started processing step", run_id=meta.run_id, job_id=meta.job_id
        )

        # Ensure the job's workspace is fully provisioned before check-in
        _ = job.folder

        try:
            # 2. Step Check-in: Update manifest immediately to 'RUNNING'
            job.check_in(current_step)

            # 3. Execute and get the next step signal (returns label or 'FINISH')
            next_step = job.execute()

            # 4. Atomic Handoff
            with self.lock:
                # Remove from current queue
                self.cache.pop(key)

                # Push to next queue if not finished
                if not next_step:
                    log.warning("Step returned no signal", next_step=next_step)
                    return

                if next_step.casefold() != "finish":
                    meta.current_step = next_step
                    meta.status = (
                        JobStatus.PENDING.value
                    )  # Ready for the next worker pool
                    new_key = f"{next_step}:{meta.job_id}:{meta.dataset_id}:{meta.run_date}:{meta.run_id}"
                    self.cache[new_key] = meta
                    log.info(
                        "Step complete. Job returned to queue.", next_step=next_step
                    )
                else:
                    log.info("Job fully completed.")
                    # Remove the job from the active queue entirely
                    self.cache.pop(key, None)
                    # Cleanup lookup key used by Orchestrator
                    lookup_key = (
                        f"active_run:{meta.job_id}:{meta.dataset_id}:{meta.run_date}"
                    )
                    self.cache.pop(lookup_key, None)

        except Exception as e:
            log.exception("Worker failed processing step")
            with self.lock:
                meta.status = JobStatus.FAILED.value
                self.cache[key] = meta
            raise e
        finally:
            self.is_busy = False

    def is_idle(self) -> bool:
        return not self.is_busy


class IngestionEngine:
    def __init__(self, exec_ctx: ExecutionContext, cache_dir: str = ".cache/ingestion"):
        self.exec_ctx = exec_ctx
        # 2. Initialize the Global Registry (Diskcache)
        # This ensures the shared cache path exists for all Ray workers
        self.registry = ServiceRegistry()
        cache_path = (self.exec_ctx.workspace_dir / DISKCACHE_FILE_PATH).resolve()

        self.cache = diskcache.Cache(cache_path)
        self.lock = FileLock(self.exec_ctx.workspace_dir / "orchestrator.lock")

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)

        # Configuration for stage limits
        self.stage_limits: dict[str, dict[str, Any]] = {
            "start": {"limit": 5, "pool": "io"},
            "extract": {"limit": 10, "pool": "io"},
            "transform": {"limit": 4, "pool": "cpu"},  # CPU-Heavy
            "audit": {"limit": 4, "pool": "io"},
            "load": {"limit": 1, "pool": "io"},  # Sequential
        }

        # Initialize specialized pools
        self.io_pool: list[ray.actor.ActorHandle] = [
            Worker.remote(f"io_{i}", self.exec_ctx) for i in range(15)  # type: ignore
        ]
        self.cpu_pool: list[ray.actor.ActorHandle] = [
            Worker.remote(f"cpu_{i}", self.exec_ctx) for i in range(4)  # type: ignore
        ]

        # Local tracker for active Ray tasks to avoid blocking RPC calls
        self._active_tasks: dict[ray.ObjectRef, ray.actor.ActorHandle] = {}

    def run(self) -> None:
        """Main loop managing multiple jobs."""
        LOG.info("Starting Orchestrator...")
        while True:
            self.scan_and_recover()
            self._process_jobs()
            time.sleep(10)  # Frequency of polling

    def queue_jobs(
        self,
        composite_key: str,
        run_id: str,
        config_file_path: str,
        run_date: str,
        current_step: str | None = None,
    ) -> None:
        """Checks config, creates jobs if not in cache, and submits them."""
        # Always start in the 'start' queue
        current_step = current_step or "start"
        queue_key = f"{current_step}:{composite_key}:{run_date}:{run_id}"
        job_id, dataset_id = composite_key.split(":", 1)

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
                meta = JobMetadata(
                    job_id=job_id,
                    run_id=run_id,
                    dataset_id=dataset_id,
                    run_date=run_date,
                    status=JobStatus.PENDING.value,
                    config_file=config_file_path,
                    current_step=current_step,
                    last_hb=time.time(),
                    expires_at=expires_at,
                )
                self.cache[queue_key] = meta
                # Set the lookup key so Orchestrator can find the run_id for this date
                self.cache[f"active_run:{job_id}:{dataset_id}:{run_date}"] = run_id

            LOG.info(
                "Queued Job",
                job_id=job_id,
                run_id=run_id,
                step=current_step,
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

                step = key.split(":", 1)[0]
                if step not in self.stage_limits:
                    continue

                # Use isinstance to narrow the type for the linter and
                # ensure runtime safety
                raw_meta = self.cache[key]
                if not isinstance(raw_meta, JobMetadata):
                    continue
                job_meta: JobMetadata = raw_meta

                if job_meta.status == JobStatus.PENDING.value:
                    limit = self.stage_limits[step]["limit"]
                    # 2. Check if the specific stage has room
                    if current_occupancy[step] < limit:
                        # 2. Select the correct Worker Pool
                        # 3. Find a free Ray worker
                        pool = (
                            self.cpu_pool
                            if self.stage_limits[step]["pool"] == "cpu"
                            else self.io_pool
                        )
                        if worker := self._get_idle_worker_from_pool(pool):
                            job_meta.status = JobStatus.PROVISIONING.value
                            job_meta.last_hb = time.time()
                            self.cache[key] = job_meta

                            # Update local occupancy count
                            current_occupancy[step] += 1

                            # Dispatch non-blocking and track the reference
                            ref = worker.process_step.remote(key, job_meta.config_file)
                            self._active_tasks[ref] = worker

    def _get_current_occupancy(self) -> dict[str, int]:
        """Counts how many workers are active in each stage."""
        counts = dict.fromkeys(self.stage_limits, 0)
        for key in list(self.cache.iterkeys()):
            if isinstance(key, str) and ":" in key:
                step = key.split(":", 1)[0]
                if step not in self.stage_limits:
                    continue

                meta = self.cache[key]
                if (
                    isinstance(meta, JobMetadata)
                    and meta.status == JobStatus.RUNNING.value
                ):
                    counts[step] += 1
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
                except Exception:
                    LOG.exception("Ray worker task failed")
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

                step, rest = key_str.split(":", 1)
                if step not in self.stage_limits:
                    continue

                raw_meta = self.cache[key]
                if not isinstance(raw_meta, JobMetadata):
                    continue

                meta: JobMetadata = raw_meta

                # Check if heartbeat is older than 5 minutes
                if (
                    meta.status == JobStatus.RUNNING.value
                    and time.time() - meta.last_hb > 300
                ):
                    LOG.warning("Zombie job detected", key=key_str)
                    self._recover_job(step, rest)

    def _recover_job(self, step_name: str, rest_of_key: str) -> None:
        """
        Recovers a stalled job by checking its physical progress.
        """
        with self.lock:
            key = f"{step_name}:{rest_of_key}"

            # .get() returns Optional[Any], so we check type and None-ness at once
            job_meta = self.cache.get(key)
            if not isinstance(job_meta, JobMetadata):
                return

        job_id = job_meta.job_id
        run_id = job_meta.run_id

        # 1. Verify if the step actually finished on disk but failed to transit
        # We check for the .success marker in the current step's folder
        if self._check_step_completion_on_disk(job_id, run_id, step_name):
            # If the symlink exists, the worker finished 'finalize' but the engine died
            next_step = self._get_next_step_name(step_name)
            LOG.info(
                "Recovery: Step was successful on disk. Promoting.",
                job_id=job_id,
                from_step=step_name,
                to_step=next_step,
            )

            del self.cache[key]
            if next_step.casefold() != "complete":
                job_meta.status = JobStatus.PENDING.value
                self.cache[f"{next_step}:{rest_of_key}"] = job_meta
        else:
            # If no symlink exists, the worker died mid-stream or before finalize.
            # Reset to PENDING in the SAME queue to allow a retry.
            LOG.info(
                "Recovery: No physical proof of success. Resetting for retry.",
                job_id=job_id,
                step=step_name,
            )
            job_meta.status = JobStatus.PENDING.value
            job_meta.last_hb = time.time()
            self.cache[key] = job_meta

    def _check_step_completion_on_disk(
        self, job_id: str, run_id: str, step_name: str
    ) -> bool:
        """
        Checks if the 'active' symlink for this step exists.
        This is the definitive proof of success in our new structure.
        """
        # Find active path
        # Logic: active/{job_id}:{dataset_id}_{run_date}/run_id/step_name
        active_root = self.exec_ctx.active_path

        active_path = find_path(active_root, run_id)
        active_path = active_path / step_name

        # It must exist and be a valid link/directory
        return bool(active_path.exists())

    def _get_next_step_name(self, current_step: str) -> str:
        """
        Decision: Use List-Index Lookup.
        Leveraging a list makes the pipeline order explicit and easy to change.
        """

        try:
            current_idx = STEP_ORDER.index(current_step)
            # Return next step, or 'complete' if we are at the end
            if current_idx + 1 < len(STEP_ORDER):
                return str(STEP_ORDER[current_idx + 1])
            return "complete"
        except ValueError:
            # If the step is unknown (e.g., job just started), start at the beginning
            return str(STEP_ORDER[0])

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
        manifest_path = active_path / "manifest.json"

        if not manifest_path.exists():
            LOG.info("No active manifest found, starting fresh.", job_id=job_id)
            return str(STEP_ORDER[0])  # Usually 'start'

        with manifest_path.open("rb") as f:
            # msgspec is fast enough to do this in the main recovery loop
            meta = msgspec.json.decode(f.read(), type=JobManifest)

            # Logic: If the current step is done, move forward.
            # Otherwise, the step crashed mid-way; resume/retry it.
            for step in STEP_ORDER:
                if hasattr(meta, step):
                    continue

                return step

        return "FINISH"

    def get_latest_manifest(self, job_id: str, run_id: str) -> JobManifest:
        """
        Decision: Direct Access.
        """
        active_root = self.exec_ctx.active_path
        active_path = find_path(active_root, run_id)
        manifest_path = active_path / "manifest.json"

        if not manifest_path.exists():
            # During recovery, if a job exists in DB but not on disk, it's a 'Ghost'
            raise FileNotFoundError(f"Active workspace missing for {job_id}")

        with manifest_path.open("rb") as f:
            # Structural validation included via msgspec
            return msgspec.json.decode(f.read(), type=JobManifest)
