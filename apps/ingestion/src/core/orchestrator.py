import sys
import time
import logging
from typing import List, Optional
from pathlib import Path

from apps.ingestion.src.core.trigger import TriggerEvent, FileTriggerEvent, TimeTriggerEvent
from dynaconf import Dynaconf

from libs.resilence.heartbeat import Heartbeat
from src.core.context.resolver import resolve_job_config
from src.core.engine import IngestionEngine
from src.core.entities.job import JobConfig
from src.utils.constants import ALWAYS_ON_MODE

LOG = logging.getLogger(__name__)
PID_FILE = Path(".daemon.pid")

#TODO: Misfire Policy

class Orchestrator:
    def __init__(self, mode: str = "SENTINEL"):
        self.mode = mode
        self.db = DatabaseConnection()
        self.heartbeat = Heartbeat()
        self.engine = IngestionEngine()
        
        self.last_heartbeat: float = 0
        self.last_db_poll: float = 0
        self.last_job_trigger: float = 0 
        self.last_engine_scan: float = 0
        

    def run(self, job_config: Optional[JobConfig] = None) -> None:
        if ALWAYS_ON_MODE:
            self._start_always_on_loop()
        else:
            self._run_synchronous_task(job_config)

    def _start_always_on_loop(self) -> None:
        """Used for Always-On Mode"""
        self.heartbeat.ready()    
        
        try:
            while True:
                now = time.time()
 
                # --- 1. Systemd Heartbeat (Every 30s) ---
                if now - self.last_heartbeat > 30:
                    self.heartbeat.ping()
                    self.last_heartbeat = now
                                
                # --- 2. Database Polling (Every 60s) ---
                if now - self.last_db_poll > 60:
                    cached_schedules = self.db.get_active_schedules()
                    self.last_db_poll = now
                    
                # --- 3. Job Triggering (Every 10s) ---
                if now - self.last_job_trigger > 10:
                    self._evaluate_triggers(cached_schedules)
                    self.last_job_trigger = now
                    
                # --- 4. Engine Driving (Every Loop - High Priority) ---
                # This drives the actual work (Ray workers/Recovery)
                if now - self.last_engine_scan > 10:
                    self.engine.scan_and_recover()
                    self.engine._process_jobs()
                    self.last_engine_scan = now
                
                # Small sleep to prevent 100% CPU usage
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def _run_synchronous_task(self, config: JobConfig) -> None:
        """The Dumb Trigger Mode logic"""
        if not config:
            raise ValueError("Dumb mode requires a valid JobConfig.")
        
        # 1. Queue the specific job
        self.engine.queue_jobs([config])
        
        # 2. Block until this specific job is finished
        composite_key = f"{config.job_id}:{config.dataset_name}"
        LOG.info(f"Monitoring job {composite_key} until completion...")
        
        while True:
            # Run the engine cycle to drive the job forward
            self.engine._process_jobs()
            
            # Check if our specific job is in the 'complete' prefix or marked COMPLETED
            # Note: We check all stage prefixes for the key
            if self._is_job_finished(composite_key):
                LOG.info(f"Job {composite_key} finished successfully.")
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

    def _evaluate_triggers(self, job_records: list[dict]) -> None:
        """
        Evaluates each job against its trigger type and 
        fans out tables to the IngestionEngine.
        """
        
        # Map of trigger types to their logic classes
        trigger_map: dict[str, TriggerEvent]= {
            "CRON": TimeTriggerEvent(),
            "FILE": FileTriggerEvent(),
            "MANUAL": lambda x: True # Ad-hoc always fires
        }

        for record in job_records:
            trigger_type = record.get("trigger_type", "CRON")
            trigger_logic: TriggerEvent = trigger_map[trigger_type]

            if trigger_logic and trigger_logic.should_fire(record):
                # 1. Load the latest YAML via Dynaconf for overrides
                # 2. Fan out tables
                # This returns a list (1 to N configs depending on table count)
                configs = resolve_job_config(
                    job_id=record["job_id"], 
                    runtime_overrides=record.get("overrides")
                )
                
                # 3. Queue to Engine (Immediate move to DiskCache)
                self.engine.queue_jobs(configs)
                
                # 4. Optional: Update DB so it doesn't trigger again immediately
                self.db.update_status(record["job_id"], "TRIGGERED")
                

    def stop(self) -> None:
        """Graceful shutdown for Always-On"""
        print("Shutting down gracefully...")
        # Close DB connections, stop Ray actors, etc.
        sys.exit(0)
