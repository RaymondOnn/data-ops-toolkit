import sys
import time
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import msgspec
import structlog

from src.core.context.job import JobContext
from src.core.engine import IngestionEngine
from src.core.models.job.base import Job, JobStatus
from src.core.state import StateStore
from src.services.database import DatabaseService
from src.utils.constants import ALWAYS_ON_MODE, JOB_STEPS_BASE_DIR
from src.utils.dates import epoch_to_iso, is_expired

from libs.resilence.heartbeat import Heartbeat

LOG = structlog.getLogger(__name__)
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
    Returns the physical path of a job based on its Approach 1 status.
    """
    base = Path(JOB_STEPS_BASE_DIR)
    
    # Branch based on Machine State
    if status in [JobStatus.FAILED, "QUARANTINE"]:
        return base / "QUARANTINE" / job_id / run_id
    elif status in (JobStatus.BLOCKED or JobStatus.DEFERRED, ):
        return base / "HOLD" / job_id / run_id
    else:
        # Standard flow uses the lowercase step name as the folder
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
    def __init__(self, db_service: DatabaseService):
        self.heartbeat = Heartbeat()
        self.last_heartbeat: float = 0
        
        self.engine = IngestionEngine()
        self.last_engine_scan: float = 0
        
        if ALWAYS_ON_MODE:
            self.state_store = StateStore(db_service)
            self.last_state_sync: float = 0        
            self.last_db_poll: float = 0
            self.last_job_trigger: float = 0 
            self.last_recovery_sweep: float = 0
            
        LOG.info("Orchestrator initialized", mode=self.mode)

    def run(self, job_id: str, overrides: dict[str, Any] | None = None) -> None:
        if ALWAYS_ON_MODE:
            self._start_always_on_loop()
            self.mode = "ALWAYS_ON"
        else:
            self._run_synchronous_task(job_id, overrides)
            self.mode = "TRIGGER"


    def _start_always_on_loop(self) -> None:
        """
        ARCHITECTURAL NOTE: We use a Polling Control Loop instead of an Event Watchdog.
        1. Portability: Works identically on EC2 (local disk) and K8S (EFS/NFS) 
        where inotify events often fail to propagate across pods.
        2. Backpressure: Prevents 'thundering herd' spikes by batching signal 
        processing into predictable 'ticks'.
        3. Self-Healing: Every tick performs a full state reconciliation, 
        ensuring we recover from crashes automatically.
        """
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
                    self._handle_expiry()
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
        Decision: Enum-driven Lifecycle Check.
        A job is finished if no associated runs are in an 'Active' state.
        """
        # 1. Physical Workspace Check (The primary breadcrumb)
        active_path = JOB_STEPS_BASE_DIR / "active" / f"{job_id}_{run_id}"
        if active_path.exists():
            return False

        # 2. Logical State Check via StateStore Mirror
        # We look for any run associated with this job_id that is still in an 'Active' state
        active_runs = [
            run for run in self.state_store._mirror.values() 
            if run["job_id"] == job_id and run["status"] in JobStatus.active_statuses()
        ]
        
        return len(active_runs) == 0

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
            from src.core.trigger import (
                FileTriggerEvent,
                TimeTriggerEvent,
                TriggerEvent,
            )

            # Map of trigger types to their logic classes
            trigger_map: dict[str, TriggerEvent]= {
                "CRON": TimeTriggerEvent(),
                "FILE": FileTriggerEvent(),
                "MANUAL": TimeTriggerEvent(), # Ad-hoc always fires
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
        from src.core.context.job import JobContextBuilder
        from src.core.models.job.steps.base import _JOB_ORDER
        
        overrides = overrides or {}
        
        # 1. Load the latest YAML via Dynaconf for overrides
        # This returns a list (1 to N configs depending on table count)
        factory = JobContextBuilder("app.yaml", "job.yaml", env="production")
        job_contexts = factory.build_job_contexts(job_id=job_id, overrides=overrides)

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
        Scans the HOLD and QUARANTINE directories to re-queue stuck jobs.
        Uses the directory structure as the source of truth when the DB is stale.
        """
        from src.core.models.job.steps.terminal import HoldStep
        
        LOG.info("Starting recovery sweep...")
        # We focus on HOLD for auto-resumption
        hold_base = JOB_STEPS_BASE_DIR / "HOLD"
        if not hold_base.exists():
            return

        # rglob finds all manifests regardless of how deep the job/run IDs are nested
        for manifest_path in hold_base.rglob("manifest.json"):
            try:
                # 1. Rehydrate the Job object from the folder metadata
                # Line 204 fix: Using the new classmethod
                job = Job.from_folder(manifest_path.parent)
                
                # 1. Check for Expiry first
                if is_expired(job.job_context.expires_at):
                    LOG.warning("Job expired", 
                                run_id=job.run_id, 
                                expiry=epoch_to_iso(job.job_context.expires_at))
                    
                    job.manifest.job_status = JobStatus.EXPIRED
                    job.save_manifest()
                    self.state_store.sync_from_folder(job.folder)
                    continue
                
                # 2. Logic check: Should this job be resumed?
                # We use a tool-based approach to check dependencies/locks
                recovery_tool = HoldStep()
                if not recovery_tool.check_and_resume(job, self.state_store):
                    continue

                LOG.info(f"Auto-recovering job {job.run_id} from HOLD.")

                # 3. Construct the composite key for the Engine
                # Line 313 fix: composite_key = "job_id:table"
                composite_key = f"{job.id}:{job.job_config.target_table}"
                LOG.info("Recovering job", run_id=job.run_id, step=job.manifest.current_step)

                # 4. Re-queue into the Ingestion Engine
                # This moves the job back into the active processing queue
                self.engine.queue_jobs(
                    composite_key=composite_key,
                    run_id=job.run_id,
                    config_file_path=str(next(job.folder.glob("*_config.json"))),
                    current_step=job.manifest.current_step 
                )

                # 5. Sync the StateStore mirror so the UI reflects the move
                # Line 341 fix: Ensure the DB knows the job is now RUNNING
                self.state_store.sync_from_folder(job.folder)

            except StopIteration:
                LOG.error(f"Recovery failed for {manifest_path}: Missing _config.json")
            except Exception as e:
                LOG.error(f"Error recovering job at {manifest_path}: {e}")

        # Final flush to Postgres to commit all recovered statuses
        self.state_store.flush()
    
    def _handle_expiry(self) -> None:
        """
        Scans for jobs that have passed their TTL and purges their workspaces.
        """
        now = time.time()
        # List to prevent 'dictionary changed size during iteration'
        runs_to_check = list(self.state_store._mirror.values())

        for data in runs_to_check:
            # We only expire jobs that are stuck in a non-terminal state
            if data["status"] in JobStatus.active_statuses():
                try:
                    # Use get_context to check expiry without a full manifest parse
                    job_path = Path(data["folder_path"])
                    if not job_path.exists():
                        continue

                    # Load only the config/context (fast)
                    ctx = self.get_context_from_path(job_path)
                    
                    if is_expired(ctx.expires_at):
                        LOG.warning("Job TTL reached. Initiating purge.", 
                                    job_id=data["job_id"], run_id=data["run_id"])
                        
                        # 1. Perform physical cleanup
                        self._cleanup_workspace(data["job_id"])
                        
                        # 2. Update State Store to terminal status
                        self.state_store.update_run(data["run_id"], {
                            "status": JobStatus.EXPIRED,
                            "step": "cleanup"
                        })
                        
                except Exception as e:
                    LOG.error("Expiry check failed", run_id=data["run_id"], error=str(e))

        self.state_store.flush()    
        
    def get_context_from_path(self, folder: Path) -> "JobContext":
        """
        Helper to load the JobContext from the active workspace.
        Standardized to look for 'config.json' directly.
        """
        from src.core.context.job import JobContext
        
        # In our refactor, we standardized the filename to config.json
        config_path = folder / "config.json"
        
        if not config_path.exists():
            # Fallback for legacy naming if necessary, otherwise stick to strict
            raise FileNotFoundError(f"Missing config.json in {folder}")

        with open(config_path, "rb") as f:
            # msgspec handles the mapping to JobContext class automatically
            return msgspec.json.decode(f.read(), type=JobContext)

    def _cleanup_workspace(self, job_id: str) -> None:
        """
        The 'Janitor' method. Deletes active links and physical data.
        """
        # 1. Remove active links
        active_path = JOB_STEPS_BASE_DIR / "active" / job_id
        if active_path.exists():
            shutil.rmtree(active_path)
        
        # 2. Remove physical data vaults (raw, transform, etc)
        # Search data/ folders for {job_id}_*
        data_root = JOB_STEPS_BASE_DIR / "data"
        for step_dir in data_root.iterdir():
            if step_dir.is_dir():
                for physical_folder in step_dir.glob(f"{job_id}_*"):
                    shutil.rmtree(physical_folder)
    
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

        # Define our signals and whether they require a deep manifest sync
        # .sync = Light heartbeat | .done = Final deep audit
        signals = {
            "*.sync": False,
            "*.done": True
        }
        
        # iterdir() returns a generator, which is memory efficient
        # This glob automatically ignores files starting with "."
        for pattern, is_deep_sync in signals.items():
            for crumb in signal_dir.glob("[!.]*.sync"):
                try:
                    # 1. Parse metadata from filename
                    # Example: 20240101-abc.transform.3.sync
                    # Metadata is now primarily in the manifest; 
                    # filename is just a pointer to the run_id
                    run_id = crumb.stem.split(".")[0]
                    # 2. Find the folder path from DB
                    run_record = self.state_store.get_run(run_id)
                    if not run_record:
                        LOG.error("Signal received for unknown run", run_id=run_id)
                        continue
                    
                    # Resolve the path using our new utility
                    physical_path = resolve_current_path(
                        job_id=run_record['job_id'],
                        run_id=run_id,
                        status=run_record['status'],
                        step=run_record['step']
                    )
                    
                    # 3. Sync manifest -> DB
                    self.state_store.sync_from_folder(
                        Path(physical_path),
                        deep_sync=is_deep_sync
                    )
                    
                    # 4. 'Eat' the breadcrumb
                    crumb.unlink(missing_ok=True)
                    LOG.debug("Signal processed", run_id=run_id)
                    
                except Exception as e:
                    LOG.error(f"Failed to process breadcrumb {crumb.name}: {e}")                
                    LOG.debug("Signal processed", run_id=run_id)
    
    def _terminate_job(self, job: Job, status: JobStatus, reason: str = None) -> None:
        """
        Controlled Crash Handler.
        Uses the Job's internal status updater to ensure consistency.
        """
        LOG.error("Terminating job", job_id=job.id, status=status, reason=reason)

        # 1. Update the Job state
        # We pass the reason as the payload so it gets serialized into the manifest
        # update_status handles the msgspec encoding and file write internally
        job.update_status(
            step_name=job.current_step, 
            status=status, 
            payload={"termination_reason": reason} if reason else None,
            is_final=True  # This triggers the .audit breadcrumb for the StateStore
        )
        
        # 2. Sync the StateStore
        # Since update_status created the .audit file, we tell the StateStore 
        # to perform its final deep sync to pull the failure details into the DB.
        self.state_store.sync_from_folder(job.id, job.run_id)
        
        # 3. Cleanup logic (Optional: move to quarantine or delete)
        if status == JobStatus.EXPIRED:
            self._cleanup_workspace(job.id)
    
    def _check_for_manual_commands(self) -> None:
        """
        Checks for 'Command Files' dropped by CLI users/scripts.
        This acts as our inter-process communication (IPC).
        """
        cmd_dir = Path(JOB_STEPS_BASE_DIR) / "signals"
        
        # Map command filenames to internal methods
        commands = {
            "RECOVER_ALL.cmd": self._handle_recovery,
            "PURGE_EXPIRED.cmd": self._handle_expiry,
            "RELOAD_CONFIG.cmd": self._reload_internal_config,
        }

        for cmd_file, method in commands.items():
            cmd_path = cmd_dir / cmd_file
            if cmd_path.exists():
                LOG.info("Manual command received", command=cmd_file)
                try:
                    # 1. Execute the command
                    method()
                    # 2. 'Eat' the command file so it doesn't run again next tick
                    cmd_path.unlink()
                except Exception as e:
                    LOG.error("Failed to execute manual command", cmd=cmd_file, error=e)