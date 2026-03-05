import sys
import time
import logging
from datetime import datetime
from typing import Any
from pathlib import Path

import msgspec

from libs.resilence.heartbeat import Heartbeat
from src.core.engine import IngestionEngine
from src.core.state import StateStore
from src.utils.constants import ALWAYS_ON_MODE, JOB_STEPS_BASE_DIR



LOG = logging.getLogger(__name__)
PID_FILE = Path(".daemon.pid")
MISFIRE_GRACE_PERIOD_SECS = 3600

def generate_run_id() -> str:
    """Generates a unique run ID for a job."""
    from nanoid import generate
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    short_hash = generate(alphabet="0123456789abcdef", size=6)
    return f"{timestamp}-{short_hash}"

def resolve_current_path(job_id: str, run_id: str, status: str, step: str) -> Path:
    """
    Deterministic path resolution based on job state.
    Ensures we look in /QUARANTINE if the DB says FAILED.
    """
    base = Path(JOB_STEPS_BASE_DIR).expanduser()
    
    # Mapping status to the top-level directory branch
    if status in ["FAILED", "QUARANTINE"]:
        return base / "QUARANTINE" / job_id / run_id
    elif status == "HELD":
        return base / "HOLD" / job_id / run_id
    else:
        # For active or completed jobs, follow the step-named folders
        # e.g., /data/ingestion/raw/my_job/run_123
        return base / step.lower() / job_id / run_id

# TODO: Check Disk Space 
# - 85%+: Mark system DEGRADED, disable Raw stage
# - 90%+: Mark system CRITICAL, alert on-call
# - Load stage continues to clear backlog)

# TODO: Backfill Runs
# TODO: Regression Testing
# TODO: Feature Toggles
# TODO: Cancel Job

class Orchestrator:
    def __init__(self, mode: str = "SENTINEL"):
        self.mode = mode

        self.heartbeat = Heartbeat()
        self.last_heartbeat: float = 0
        
        self.engine = IngestionEngine()
        self.last_engine_scan: float = 0
        
        if ALWAYS_ON_MODE:
            self.state_store = StateStore()
            self.last_state_sync: float = 0        
            self.last_db_poll: float = 0
            self.last_job_trigger: float = 0 
            self.last_recovery_sweep: float = 0
            
        

    def run(self, job_id: str, overrides: dict[str, Any] | None = None) -> None:
        if ALWAYS_ON_MODE:
            self._start_always_on_loop()
        else:
            self._run_synchronous_task(job_id, overrides)

    def _start_always_on_loop(self) -> None:
        """Used for Always-On Mode"""
        self.heartbeat.ready()    
        
        try:
            while True:
                now = time.time()
                self._process_worker_signals()
 
                # --- 1. Systemd Heartbeat (Every 30s) ---
                if now - self.last_heartbeat > 30:
                    self.heartbeat.ping()
                    self.last_heartbeat = now
                                
                # --- 2. Database Polling (Every 60s) ---
                if now - self.last_db_poll > 60:                  
                    # state_store.refresh() queries Postgres for active job definitions
                    self.state_store.refresh() 
                    self._evaluate_triggers(self.state_store.get_active_definitions())
                    self.last_db_poll = now
                    
                
                # 2. NEW: Sync the StateStore to Postgres
                # This writes all buffered 'RUNNING', 'HELD', or 'COMPLETED' updates
                if now - self.last_state_sync > 30:
                    self.state_store.flush()
                    self.last_state_sync = now
                    
                # # --- 3. Job Triggering (Every 10s) ---
                # if now - self.last_job_trigger > 10:
                #     self.last_job_trigger = now
                    
                # --- 4. Engine Driving (Every Loop - High Priority) ---
                # This drives the actual work (Ray workers/Recovery)
                if now - self.last_engine_scan > 10:
                    self.engine.scan_and_recover()
                    self.engine._process_jobs()
                    self.last_engine_scan = now
                    
                # 3. Maintenance: Run recovery sweep every 5 minutes
                if now - self.last_recovery_sweep > 300:
                    self._handle_recovery()
                    self.last_recovery_sweep = time.time()
                
                # Small sleep to prevent 100% CPU usage
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def _run_synchronous_task(
        self, 
        job_id: str, 
        overrides: dict[str, Any] | None = None
    ) -> None:
        """The Dumb Trigger Mode logic"""
        if not overrides:
            raise ValueError("Dumb mode requires a valid JobConfig.")
        
        self._trigger_job(job_id, overrides)
        
        # 2. Block until this specific job is finished
        LOG.info(f"Monitoring job {job_id} until completion...")
        
        while True:
            # Run the engine cycle to drive the job forward
            self.engine._process_jobs()
            
            # Check if our specific job is in the 'complete' prefix or marked COMPLETED
            # Note: We check all stage prefixes for the key
            
            if self._is_job_finished(job_id):
                LOG.info(f"Job {job_id} finished successfully.")
                break
            
            time.sleep(2)
            
    def _is_job_finished(self, job_id: str) -> bool:
        """
        Checks if all tables associated with a job_id have cleared the pipeline.
        """
        # Define all prefixes that represent 'active' work
        active_stages = ["start", "raw", "transform", "audit", "load"]
        
        with self.engine.lock:
            for key in self.engine.cache.iterkeys():
                # Key format is 'stage:job_id:table_name'
                if ":" in key:
                    prefix, k_job_id, table = key.split(":", 2)
                    if k_job_id == job_id and prefix in active_stages:
                        # Found at least one table still in progress
                        return False
        
        # If we get here, no keys for this job_id exist in active stages
        return True

    def _evaluate_triggers(self, job_records: list[dict[str, Any]]) -> None:
        """
        Evaluates each job against its trigger type and 
        fans out tables to the IngestionEngine.
        
        Misfire Policy is handled here based on GRACE_PERIOD_SECS:
        -1: Fire Immediately (Always True)
        0 : Skip (Always False if delay > 0)
        >0: Grace Period (True if within bounds)
        """
        def provision_run(record: dict[str, Any]) -> None:
            from src.core.trigger import TriggerEvent, FileTriggerEvent, TimeTriggerEvent
            
            # Map of trigger types to their logic classes
            trigger_map: dict[str, TriggerEvent]= {
                "CRON": TimeTriggerEvent(),
                "FILE": FileTriggerEvent(),
                "MANUAL": lambda x: True # Ad-hoc always fires
            }
            
            
            trigger_type = record.get("trigger_type", "CRON")
            if trigger_map[trigger_type].should_fire(record):
                self._trigger_job(job_id=record["job_id"])
        
        now = time.time()

        for record in job_records:
            # If the job was JUST recovered, its next_scheduled_time was set to 'now'
            # by TerminalStep.recover(), so delay will be ~0.
            scheduled_time = record.get("next_scheduled_time", now)
            if not scheduled_time:
                continue

            # Resolve grace period: Config override > System Default
            # -1 = Always Fire | 0 = Strict Skip | >0 = Threshold
            grace_sec = record.get("misfire_grace_sec", )
            delay = now - scheduled_time
            
            # --- MISFIRE POLICY EVALUATION ---
            # CASE 1: The job is LATE (delay > 0)
            if delay > 0: 
                
                # A: If grace_sec is -1, it's a "Manual Resume" or "Force Run"
                # Policy -1 means 'run no matter how late we are'
                if MISFIRE_GRACE_PERIOD_SECS == -1:
                    LOG.info(f"[FORCE_RUN]: Job {record['job_id']} is {delay}s late. Policy: -1")
                    provision_run(record)
                    continue
                    
                # B: If delay is within the grace period (delay < grace_sec)
                if delay <= MISFIRE_GRACE_PERIOD_SECS:
                    LOG.info(f"[CATCH_UP]]: Job {record['job_id']} within grace ({delay}s < {grace_sec}s)")
                    provision_run(record)
                    continue
                    
                # C: If delay is beyond the grace period
                if delay > MISFIRE_GRACE_PERIOD_SECS:
                    LOG.warning(f"[EXPIRED]: Job {record['job_id']} delayed by {delay}s. Policy: {grace_sec}s")
                    # We tell the StateStore to update the next run time without executing
                    self.state_store.skip_misfired_run(record["job_id"])
                    continue

            # CASE 2: The job is ON TIME (Standard Trigger Logic)
            provision_run(record)
            
                
    def stop(self) -> None:
        """Graceful shutdown for Always-On"""
        print("Shutting down gracefully...")
        # Close DB connections, stop Ray actors, etc.
        sys.exit(0)
        
    def _trigger_job(
        self, 
        job_id: str, 
        overrides: dict[str, Any] | None = None
    ) -> None:
        from src.core.context.job import resolve_job_context
        from src.core.entities.job.steps.base import _JOB_ORDER
        
        overrides = overrides or {}
        
        # 1. Load the latest YAML via Dynaconf for overrides
        # This returns a list (1 to N configs depending on table count)
        job_contexts = resolve_job_context(
            job_id=job_id, 
            runtime_overrides=overrides
        )

        for job_ctx in job_contexts:
            #2. Create Initial Folder Structure
            job_cfg_dir = Path(JOB_STEPS_BASE_DIR) / _JOB_ORDER[0]
            job_cfg_dir.mkdir(parents=True)
        
            # 3. Freeze the Context into the folder
            run_id = generate_run_id()
            composite_key = f"{job_ctx.job_id}:{job_ctx.table}"
            job_cfg_file = f"{composite_key}_{run_id}_config.json"
            with open(job_cfg_dir / job_cfg_file, "wb") as f:
                f.write(msgspec.json.encode(job_ctx))
                
            # 4. Queue to Engine (Immediate move to DiskCache)
            self.engine.queue_jobs(
                composite_key=composite_key,
                run_id=run_id,
                config_file_path=str(job_cfg_dir),
            )
            
            # 5. Optional: Update DB so it doesn't trigger again immediately
            self.state_store.update_status(job_id, "TRIGGERED")
            self.last_job_trigger = time.time()
            

    def _handle_recovery(self) -> None:
        """
        Scans the HOLD directory to auto-resume deferred jobs.
        Note: QUARANTINE is ignored here as it requires manual CLI intervention.
        """
        from src.core.entities.job.steps.terminal import HoldStep
        from src.core.entities.job.base import Job
        
        hold_base = Path(JOB_STEPS_BASE_DIR).expanduser() / "HOLD"
        if not hold_base.exists():
            return

        # Look for any manifest in the HOLD tree
        for manifest_path in hold_base.rglob("manifest.json"):
            # We load the job from the folder (it already has config.json inside!)
            job = Job.from_folder(manifest_path.parent) 
            
            # HoldStep encapsulates the 'ping' logic and the 'recover' move
            recovery_tool = HoldStep()
            if recovery_tool.check_and_resume(job, self.state_store):
                LOG.info(f"Auto-recovered job {job.run_id} from HOLD. Moving to engine.")
                
                # Re-queue the recovered job into the engine
                self.engine.queue_job(
                    job_id=job.id,
                    run_id=job.run_id,
                    folder_path=str(job.folder),
                    current_step=job.manifest.current_step 
                )
                
    def _process_worker_signals(self) -> None:
        """
        Scans the flat signals directory for any {run_id}.signal files.
        
        The worker writes the manifest.json first, then "drops" the signal file. 
        This prevents the Orchestrator from reading a manifest that the worker 
        is still writing.
        """
        signal_dir = Path(JOB_STEPS_BASE_DIR) / "signals"
        if not signal_dir.exists():
            return

        # iterdir() returns a generator, which is memory efficient
        # This glob automatically ignores files starting with "."
        for crumb in signal_dir.glob("[!.]*.sync"):
            try:
                # 1. Parse metadata from filename
                # Example: 20240101-abc.transform.3.sync
                parts = crumb.stem.split(".")
                run_id = parts[0]
                
                # 2. Find the folder path from DB
                run_record = self.state_store.get_run(run_id)
                if run_record:
                    # Resolve the path using our new utility
                    physical_path = resolve_current_path(
                        job_id=run_record['job_id'],
                        run_id=run_id,
                        status=run_record['status'],
                        step=run_record['step']
                    )
                    
                    # 3. Sync manifest -> DB
                    self.state_store.sync_from_folder(Path(physical_path))
                
                # 4. 'Eat' the breadcrumb
                crumb.unlink(missing_ok=True)
                
            except Exception as e:
                LOG.error(f"Failed to process breadcrumb {crumb.name}: {e}")