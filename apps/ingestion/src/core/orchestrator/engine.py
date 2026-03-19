import time
from pathlib import Path
from typing import Any

import diskcache
import msgspec
import ray
import structlog
from filelock import FileLock

from src.core.models.job import Job, JobManifest
from src.core.models.steps import _JOB_ORDER
from src.services.registry import ServiceRegistry
from src.utils.constants import JOB_STEPS_BASE_DIR, DISKCACHE_FILE_PATH
from src.utils.common import find_path


LOG = structlog.getLogger(__name__)
LOCK_FILE = "/tmp/orchestrator.lock"
CACHE_DIR = ".cache/ingestion"


@ray.remote
class Worker:
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.cache = diskcache.Cache(
            DISKCACHE_FILE_PATH,
            timeout=10,  # Increase timeout for slow PV file locks (NFS/EFS)
            settings={
                "sqlite_journal_mode": "wal"
            },  # Ensure WAL mode is active for concurrent reads/writes
        )
        self.lock = FileLock(LOCK_FILE)
        self._busy = False

    def process_step(self, key: str, config_file_path: str | Path) -> None:
        current_step, composite_key = key.split(":", 1)

        # 1. Rehydrate Job
        with self.lock:
            meta = self.cache[key]

        job: Job = Job(
            composite_key=composite_key,
            run_id=meta["run_id"],
            run_date=meta["run_date"],
            worker_id=self.worker_id,
            target_step=current_step,
        )
        self.is_busy = True

        try:
            # 2. Execute the single step
            job.execute()

            # 3. Get the next step signal
            next_step = job._step._transit(job)

            # 4. Atomic Handoff
            with self.lock:
                # Remove from current queue
                self.cache.pop(f"{current_step}:{composite_key}")

                # Push to next queue if not finished
                if next_step != "complete":
                    meta["current_step"] = next_step
                    meta["status"] = "PENDING"  # Ready for the next worker pool
                    self.cache[f"{next_step}:{composite_key}"] = meta
                else:
                    LOG.info(f"Job {composite_key} fully completed.")
                    meta["status"] = "COMPLETED"
                    self.cache[f"{current_step}:{composite_key}"] = meta

        except Exception as e:
            with self.lock:
                meta["status"] = "FAILED"
                self.cache[f"{current_step}:{composite_key}"] = meta
            raise e
        finally:
            self.is_busy = False

    def is_idle(self) -> bool:
        return not self.is_busy


class IngestionEngine:
    def __init__(self, cache_dir: str = ".cache/ingestion"):
        # 2. Initialize the Global Registry (Diskcache)
        # This ensures the shared cache path exists for all Ray workers
        self.registry = ServiceRegistry()

        self.cache = diskcache.Cache(DISKCACHE_FILE_PATH)
        self.lock = FileLock(f"{cache_dir}/orchestrator.lock")

        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)

        # Configuration for stage limits
        self.stage_limits: dict[str, dict[str, Any]] = {
            "start": {"limit": 5, "pool": "io"},
            "raw": {"limit": 10, "pool": "io"},
            "transform": {"limit": 4, "pool": "cpu"},  # CPU-Heavy
            "audit": {"limit": 4, "pool": "io"},
            "load": {"limit": 1, "pool": "io"},  # Sequential
        }

        # Initialize specialized pools
        self.io_pool: list[ray.actor.ActorHandle] = [Worker.remote(f"io_{i}") for i in range(15)]
        self.cpu_pool: list[ray.actor.ActorHandle] = [Worker.remote(f"cpu_{i}") for i in range(4)]

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
        current_step: str | None = None,
    ) -> None:
        """Checks config, creates jobs if not in cache, and submits them."""
        from src.utils.dates import epoch_to_iso, get_end_of_day_ts

        # Always start in the 'start' queue
        current_step = current_step or "start"
        queue_key = f"{current_step}:{composite_key}"
        job_id, table = composite_key.split(":", 1)

        # Logic to determine if this is a snapshot (e.g., based on job naming convention)
        is_snapshot = "snapshot" in job_id.lower()
        expires_at = get_end_of_day_ts() if is_snapshot else None

        # Log the dispatch with human-readable timestamps
        expiry_str = epoch_to_iso(expires_at) if expires_at else "NEVER"

        with self.lock:
            # Check if job exists in cache
            if queue_key not in self.cache:
                # Brand new entry
                self.cache[queue_key] = {
                    "job_id": job_id,
                    "run_id": run_id,
                    "status": "PENDING",
                    "config_file": config_file_path,
                    "table_name": table,
                    "current_step": current_step,  # Default start
                    "last_hb": time.time(),
                    "retry_count": 0,
                }
            LOG.info(
                f"Queued Job: {job_id} | RunID: {run_id} | "
                f"Step: {current_step} | Expires: {expiry_str}"
            )

    def _process_jobs(self) -> None:
        # 1. Calculate current occupancy per stage
        # We count how many keys per prefix have status 'RUNNING'
        current_occupancy = self._get_current_occupancy()

        with self.lock:
            for key in self.cache.iterkeys():
                if ":" not in key:
                    continue
                step, composite_key = key.split(":", 1)
                job_meta = self.cache[key]

                if job_meta["status"] == "PENDING":
                    limits = min(self.stage_limits[step]["limit"], 1)
                    # 2. Check if the specific stage has room
                    if current_occupancy[step] < limits:
                        continue

                    # 2. Select the correct Worker Pool
                    # 3. Find a free Ray worker
                    pool = self.cpu_pool if limits["pool"] == "cpu" else self.io_pool
                    if worker := self._get_idle_worker_from_pool(pool):
                        job_meta["status"] = "SUBMITTED"
                        job_meta["last_hb"] = time.time()
                        self.cache[key] = job_meta

                        # Update local occupancy count
                        current_occupancy[step] += 1

                        # Pass the key and the job_meta to the Ray Task
                        worker.process_step.remote(key, job_meta["config_file"])

    def _get_current_occupancy(self) -> dict[str, int]:
        """Counts how many workers are active in each stage."""
        counts = {k: 0 for k in self.stage_limits.keys()}
        for key in self.cache.iterkeys():
            if ":" in key:
                step = key.split(":")[0]
                if self.cache[key]["status"] == "RUNNING":
                    counts[step] = counts.get(step, 0) + 1
        return counts

    def _get_idle_worker_from_pool(self, pool: list[ray.actor.ActorHandle]) -> Any | None:
        for w in pool:
            if ray.get(w.is_idle.remote()):
                return w
        return None

    # TODO: Handle Timeouts
    def scan_and_recover(self) -> None:
        """Scans all stage queues for zombie jobs."""
        with self.lock:
            for key in list(self.cache.iterkeys()):
                if ":" not in key:
                    continue

                step, composite_key = key.split(":", 1)
                meta = self.cache[key]

                if meta.get("status") == "RUNNING":
                    # Check if heartbeat is older than 5 minutes
                    if time.time() - meta.get("last_hb", 0) > 300:
                        LOG.warn("Zombie job detected", key=key)
                        self._recover_job(step, composite_key)

    def _recover_job(self, step_name: str, composite_key: str) -> None:
        """
        Recovers a stalled job by checking its physical progress.
        """
        with self.lock:
            key = f"{step_name}:{composite_key}"
            job_meta = self.cache.get(key)
            if not job_meta:
                return

        job_id = job_meta["job_id"]
        run_id = job_meta["run_id"]

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
            if next_step != "complete":
                job_meta["status"] = "PENDING"
                self.cache[f"{next_step}:{composite_key}"] = job_meta
        else:
            # If no symlink exists, the worker died mid-stream or before finalize.
            # Reset to PENDING in the SAME queue to allow a retry.
            LOG.info(
                "Recovery: No physical proof of success. Resetting for retry.",
                job_id=job_id,
                step=step_name,
            )
            job_meta["status"] = "PENDING"
            job_meta["last_hb"] = time.time()
            self.cache[key] = job_meta

    def _check_step_completion_on_disk(self, job_id: str, run_id: str, step_name: str) -> bool:
        """
        Checks if the 'active' symlink for this step exists.
        This is the definitive proof of success in our new structure.
        """
        # Find active path
        # Logic: active/{job_id}:{dataset_id}_{run_date}/run_id/step_name
        # Example: active/job_123:dataset_456_2022-01-01/run_12345/transform
        active_root = JOB_STEPS_BASE_DIR / "active"

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
            current_idx = _JOB_ORDER.index(current_step)
            # Return next step, or 'complete' if we are at the end
            if current_idx + 1 < len(_JOB_ORDER):
                return str(_JOB_ORDER[current_idx + 1])
            return "complete"
        except ValueError:
            # If the step is unknown (e.g., job just started), start at the beginning
            return str(_JOB_ORDER[0])

    def _recover_from_manifest(self, job_id: str, run_id: str) -> str:
        """
        Decision: Single-File Peep.
        Peeps at the manifests on disk to find where the job stalled.
        Used during System Recovery or Orchestrator Boot-up to decide exactly
        where to resume a job that was interrupted.
        Instead of searching 5+ folders, we read the one 'active' manifest.
        This is O(1) instead of O(N).
        """
        from src.utils.common import find_path

        active_root = JOB_STEPS_BASE_DIR / "active"
        active_path = find_path(active_root, run_id)
        manifest_path = active_path / "manifest.json"

        if not manifest_path.exists():
            LOG.info("No active manifest found, starting fresh.", job_id=job_id)
            return str(_JOB_ORDER[0])  # Usually 'start'

        with open(manifest_path, "rb") as f:
            # msgspec is fast enough to do this in the main recovery loop
            meta = msgspec.json.decode(f.read(), type=JobManifest)

            # Logic: If the current step is done, move forward.
            # Otherwise, the step crashed mid-way; resume/retry it.
            if meta.step_status == "COMPLETED":
                return self._get_next_step_name(meta.current_step)

            return str(meta.current_step)

    def get_latest_manifest(self, job_id: str, run_id: str) -> JobManifest:
        """
        Decision: Direct Access.
        Finds the evolving manifest for the job in its active workspace.
        """
        active_root = JOB_STEPS_BASE_DIR / "active"
        active_path = find_path(active_root, run_id)
        manifest_path = active_path / "manifest.json"

        if not manifest_path.exists():
            # During recovery, if a job exists in DB but not on disk, it's a 'Ghost'
            raise FileNotFoundError(f"Active workspace missing for {job_id}")

        with open(manifest_path, "rb") as f:
            # Structural validation included via msgspec
            return msgspec.json.decode(f.read(), type=JobManifest)
