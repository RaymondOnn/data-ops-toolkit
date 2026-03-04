from datetime import datetime
import logging
import time
from pathlib import Path
from typing import Any, Optional


import diskcache
import ray
import msgspec
from filelock import FileLock
from nanoid import generate

from src.core.entities.job import Job
from src.core.context.job import JobContext
from src.core.entities.job.manifest import BaseManifest


LOG = logging.getLogger(__name__)
LOCK_FILE = "/tmp/orchestrator.lock"
CACHE_DIR = ".cache/ingestion"

@ray.remote
class JobWorker:
    def __init__(self, worker_id: str):
        self.worker_id = worker_id # TODO: check how this worker_id works
        self.cache = diskcache.Cache(CACHE_DIR)
        self.lock = FileLock(LOCK_FILE)
        self._busy = False
    
    #TODO: Check that folder is brought over when job is picked up
    def process_step(self, current_step: str, composite_key: str) -> None:
        # 1. Rehydrate Job
        with self.lock:
            meta = self.cache[f"{current_step}:{composite_key}"]
        
        job = Job(meta["job_id"], meta["config"], start_step=current_step)
        self.is_busy = True
        
        try:
            # 2. Execute the single step
            job.execute()
            
            # 3. Get the next step signal
            next_step = job.step.transit(job)
            
            # 4. Atomic Handoff
            with self.lock:
                # Remove from current queue
                self.cache.pop(f"{current_step}:{composite_key}")
                
                # Push to next queue if not finished
                if next_step != "complete":
                    meta["current_step"] = next_step
                    meta["status"] = "PENDING" # Ready for the next worker pool
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
        self.cache = diskcache.Cache(cache_dir)
        self.lock = FileLock(f"{cache_dir}/orchestrator.lock")
        
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
        
        # Configuration for stage limits
        self.stage_limits = {
            "start":     {"limit": 5,  "pool": "io"},
            "raw":       {"limit": 10, "pool": "io"},
            "transform": {"limit": 4,  "pool": "cpu"}, # CPU-Heavy
            "audit":     {"limit": 4,  "pool": "io"},
            "load":      {"limit": 1,  "pool": "io"}  # Sequential
        }
        
        # Initialize specialized pools
        self.io_pool = [JobWorker.remote(f"io_{i}") for i in range(15)]
        self.cpu_pool = [JobWorker.remote(f"cpu_{i}") for i in range(4)]
    
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
            # Check if the config defines multiple tables/datasets
            datasets = config.get("tables", [config.dataset_name])
            
            for table in datasets:
                # 1. Generate the 2026-style run_id
                timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
                short_hash = generate(alphabet="0123456789abcdef", size=6)
                run_id = f"{timestamp}-{short_hash}"
                
                
                # Granularity: Job Date + Dataset Name
                composite_key = f"{config.job_id}:{config.dataset_name}"
                # Always start in the 'start' queue
                queue_key = f"start:{composite_key}"
                
                with self.lock:
                    # Check if job exists in cache
                    if queue_key not in self.cache:
                        # Brand new entry
                        self.cache[queue_key] = {
                            "job_id": job_id,
                            "run_id": run_id,
                            "config": config,
                            "status": "PENDING",
                            "table_name": table,
                            "current_step": "start", # Default start
                            "last_hb": time.time(),
                            "retry_count": 0
                        }
                    LOG.info(f"Queued {config.dataset_name} | RunID: {run_id}")
                    
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
                    limits = self.stage_limits.get(step, {})
                    # 2. Check if the specific stage has room
                    if current_occupancy[step] < limits.get(step, 1):
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
                        worker.process_step.remote(key, job_meta)
                    
    def _get_current_occupancy(self) -> dict:
        """Counts how many workers are active in each stage."""
        counts = {k: 0 for k in self.stage_limits.keys()}
        for key in self.cache.iterkeys():
            if ":" in key:
                step = key.split(":")[0]
                if self.cache[key]["status"] == "RUNNING":
                    counts[step] = counts.get(step, 0) + 1
        return counts

    def _get_idle_worker_from_pool(self, pool: list[JobWorker]) -> Any | None:
        for w in pool:
            if ray.get(w.is_idle.remote()):
                return w
        return None
    
    def scan_and_recover(self) -> None:
        """Scans all stage queues for zombie jobs."""
        with self.lock:
            for key in list(self.cache.iterkeys()):
                if ":" not in key: 
                    continue
                step, composite_key = key.split(":", 1)
                meta = self.cache[key]

                if meta["status"] == "RUNNING":
                    # Check if heartbeat is older than 5 minutes
                    if time.time() - meta.get("last_hb", 0) > 300:
                        self._recover_job(step, composite_key)

    def _recover_job(self, step_prefix: str, composite_key: str) -> None:
        """
        Recovers a stalled job by checking its physical progress.
        """
        with self.lock:
            key = f"{step_prefix}:{composite_key}"
            job_meta = self.cache.get(key)
            if not job_meta:
                return

        # 1. Verify if the step actually finished on disk but failed to transit
        # We check for the .success marker in the current step's folder
        if self._check_step_completion_on_disk(step_prefix, job_meta):
            # If disk says it's done, move it to the NEXT step queue
            next_step = self._get_next_step_name(step_prefix)
            LOG.info(f"Recovery: Moving {composite_key} from {step_prefix} to {next_step}")
            
            del self.cache[key]
            if next_step != "complete":
                job_meta["status"] = "PENDING"
                self.cache[f"{next_step}:{composite_key}"] = job_meta
        else:
            # If disk says it's NOT done, reset to PENDING in the SAME queue
            LOG.info(f"Recovery: Resetting {composite_key} in {step_prefix} queue")
            job_meta["status"] = "PENDING"
            job_meta["last_hb"] = time.time()
            self.cache[key] = job_meta
    
    def _check_step_completion_on_disk(self, step_prefix: str, meta: dict) -> bool:
        """
        Scans the output_path for the .success file created by JobStep.mark_success.
        """
        # Using Pathlib to check: <output_path>/<StepClass>/<job_id>/<run_id>/<dataset>/*.success
        # Note: You'll need to map step_prefix (e.g. 'raw') to Class Name (e.g. 'RawStep')
        step_class_name = f"{step_prefix.capitalize()}Step"
        
        base_path = Path(meta["config"].output_path)
        search_path = (
            base_path / step_class_name / meta["job_id"] / meta["run_id"] / meta["config"].dataset_name
        )
        
        return any(search_path.glob("*.success"))

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
 
    def _recover_from_manifest(self, job_id: str, run_id: str, output_path: str) -> str:
        """
        Peeps at the manifests on disk to find where the job stalled.
        """
        # Steps in reverse order to find the latest state
        for step in ["load", "audit", "transform", "raw"]:
            manifest_path = Path(output_path) / step / job_id / run_id / "manifest.json"
            
            if manifest_path.exists():
                # Peep: msgspec is fast enough to do this inside the Orchestrator loop
                with open(manifest_path, "rb") as f:
                    # We can decode into BaseManifest just to see the 'status' and 'step'
                    meta = msgspec.json.decode(f.read(), type=BaseManifest)
                    
                    if meta.status == "COMPLETED":
                        # If 'raw' is completed, we should queue 'transform'
                        return self._get_next_step_name(step)
                    else:
                        # If it's 'PENDING' or 'RUNNING', it crashed mid-step. Resume this step.
                        return step
        return "raw" # Default start
    
    def get_latest_manifest(self, context: JobContext) -> JobManifest:
        """
        Finds the furthest reached step and returns its manifest.
        """
        # Search backwards from the end of the pipeline
        for step in ["load", "audit", "transform", "raw", "start"]:
            manifest_path = (
                Path(context.output_path) / step / context.job_id / context.run_id / "manifest.json"
            )
            
            if manifest_path.exists():
                # msgspec.json.decode is extremely fast
                with open(manifest_path, "rb") as f:
                    return msgspec.json.decode(f.read(), type=JobManifest)
        
        raise FileNotFoundError(f"No manifest found for {context.job_id}")