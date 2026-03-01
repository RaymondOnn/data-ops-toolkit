import logging
import time
from pathlib import Path
from typing import Any
import uuid

import diskcache
import ray
from filelock import FileLock

from src.core.entities.job import Job, CompleteStep, JobConfig

LOG = logging.getLogger(__name__)
LOCK_FILE = "/tmp/orchestrator.lock"
CACHE_DIR = ".cache/ingestion"


class IngestionEngine:
    def __init__(self, cache_dir: str = ".cache/ingestion"):
        self.cache = diskcache.Cache(cache_dir)
        self.lock = FileLock(f"{cache_dir}/orchestrator.lock")
        
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
            
        # Initialize a pool of workers
        self.workers = [JobWorker.remote() for _ in range(4)] # e.g., 4 workers
    
    def run(self) -> None:
        """Main loop managing multiple jobs."""
        LOG.info("Starting Orchestrator...")
        while True:
            self.scan_and_recover()
            self._process_jobs()
            time.sleep(10) # Frequency of polling

    def queue_jobs(self, configs: list[JobConfig]) -> None:
        """Checks config, creates jobs if not in cache, and submits them."""
        for config in configs:
            # Granularity: Job Date + Dataset Name
            job_id = config.job_id
            composite_key = f"{config.job_id}:{config.dataset_name}"
            
            with self.lock:
                # Check if job exists in cache
                if composite_key not in self.cache:
                    # Brand new entry
                    self.cache[composite_key] = {
                        "job_id": job_id,
                        "run_id": str(uuid.uuid4())[:8],
                        "config": config,
                        "status": "PENDING",
                        "current_step": "start", # Default start
                        "last_hb": time.time(),
                        "recovery_count": 0
                    }
                LOG.info(f"Queued job instance for dataset: {config.dataset_name}")
                    
    def _process_jobs(self) -> None:
        with self.lock:
            for key in self.cache.iterkeys():
                job_meta = self.cache[key]
                if job_meta["status"] == "PENDING":
                    
                    # Simple round-robin allocation
                    worker = self.workers[hash(key) % len(self.workers)]
                    job_meta["status"] = "SUBMITTED"
                    job_meta["last_hb"] = time.time()
                    self.cache[key] = job_meta
                    # Pass the key and the job_meta to the Ray Task
                    worker.process_job.remote(key, job_meta)
                    
    def scan_and_recover(self) -> None:
        """Identifies dead jobs and resets them."""
        now = time.time()
        with self.lock:
            for key in self.cache.iterkeys():
                # Assuming jobs are keyed by job_id
                job_meta = self.cache.get(key)
                if job_meta and job_meta["status"] == "RUNNING":
                    if now - job_meta["last_hb"] > 300:  # 5 minutes
                        self._recover_job(key)

    def _recover_job(self, composite_key: str) -> None:
        """
        composite_key is 'job_id:dataset_name'
        """
        job_meta = self.cache.get(composite_key)
        if not job_meta:
            return

        config = job_meta["config"]
        # Determine the last successful step by scanning the filesystem
        last_step_name = self._find_last_completed_step(job_meta)
        
        LOG.info(f"Recovering {composite_key}. Last successful step: {last_step_name}")

        # Transition to the NEXT step after the last successful one
        next_step = self._get_next_step_name(last_step_name)
        
        # Reset the job in the cache so the orchestrator picks it up
        job_meta["status"] = "PENDING"
        job_meta["start_step"] = next_step
        job_meta["recovery_count"] = job_meta.get("recovery_count", 0) + 1
        self.cache[composite_key] = job_meta
        
    def _find_last_completed_step(self, job_meta: dict[str, Any]) -> str:
        """
        Scans the output_path for .success markers to find the furthest progress.
        """
        # Order of operations
        steps = ["start", "raw", "transform", "audit", "load"]
        last_completed = "start"
        
        config = job_meta["config"]
        base_path = Path(config.output_path)

        for step in steps:
            # We look for the marker: <step_name>/<job_id>/<run_id>/<dataset>/*.success
            # This matches the logic in JobStep._get_checkpoint_path
            marker_pattern = f"{step}/{job_meta['job_id']}/{job_meta['run_id']}/{config.dataset_name}/*.success"
            markers = list(base_path.glob(marker_pattern))
            
            if markers:
                last_completed = step
            else:
                # If a step is missing a marker, the previous one was the last successful one
                break
                
        return last_completed

    def _get_next_step_name(self, current_step: str) -> str:
        workflow = {
            "start": "raw",
            "raw": "transform",
            "transform": "audit",
            "audit": "load",
            "load": "complete"
        }
        return workflow.get(current_step, "start")
 
@ray.remote
class JobWorker:
    def __init__(self) -> None:
        self.cache = diskcache.Cache(CACHE_DIR)
        self.lock = FileLock(LOCK_FILE)

    def process_job(self, composite_key: str) -> None:
        """Executes a single job synchronously."""
        cache: diskcache.Cache = diskcache.Cache(CACHE_DIR)
        
        # 1. Fetch metadata and mark as running
        with FileLock(LOCK_FILE):
            job_meta = cache[composite_key]
            job_meta["status"] = "RUNNING"
            job_meta["last_hb"] = time.time()
            cache[composite_key] = job_meta

        # 2. Rehydrate the Job instance using cached config and start_step
        # This uses the Job.__init__ logic you provided to set the correct _step
        job = Job(
            job_id=job_meta["job_id"],
            job_config=job_meta["config"],
            start_step=job_meta.get("current_step", "start"),
            run_id=job_meta.get("run_id")
        )

        # 2. Execute steps
        try:
            while not isinstance(job.step, CompleteStep):
                LOG.info(f"Job {composite_key} executing step: {job.step.__class__.__name__}")
                job.execute()
                
                # Update heartbeat during execution
                with FileLock(LOCK_FILE):
                    job_meta = cache[composite_key]
                    job_meta["last_hb"] = time.time()
                    cache[composite_key] = job_meta

            # 3. Mark complete
            with FileLock(LOCK_FILE):
                job_meta = cache[composite_key]
                job_meta["status"] = "COMPLETED"
                cache[composite_key] = job_meta

        except Exception as e:
            LOG.error(f"Job {composite_key} failed: {e}")
            with FileLock(LOCK_FILE):
                job_meta = cache[composite_key]
                job_meta["status"] = "FAILED"
                cache[composite_key] = job_meta
