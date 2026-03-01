import sys
import argparse
import time
from typing import List, Optional
from pathlib import Path

from libs.resilence.heartbeat import Heartbeat
from apps.ingestion.src.utils.constants import ALWAYS_ON_MODE
PID_FILE = Path(".daemon.pid")

class Orchestrator:
    def __init__(self, mode: str = "SENTINEL"):
        self.mode = mode
        self.db = DatabaseConnection()
        self.heartbeat = Heartbeat()
        self.last_heartbeat: float = 0
        self.last_db_poll: float = 0
        self.last_job_trigger: float = 0 



    def run(self) -> None:
        if ALWAYS_ON_MODE:
            self.start()
        else:
            self.run_once()

    def start(self) -> None:
        """Used for Always-On Mode"""
        print("Starting Sentinel Mode...")
        self.heartbeat.ready()
        
        try:
            while True:
                now = time.time()
                
                # 1. Look for the 'Two Earliest' jobs in DB
                jobs: List[Optional[Job]] = self.db.get_next_queued_jobs(limit=2)
                for job in jobs:
                    if job:
                        self._execute_logic(job)
                
                # 2. Heartbeat for Systemd
                # --- 1. Systemd Heartbeat (Every 30s) ---
                if now - self.last_heartbeat > 30:
                    self.heartbeat.ping()
                    self.last_heartbeat = now
                
                
                # --- 2. Database Polling (Every 60s) ---
                if now - self.last_db_poll > 60:
                    self._poll_database()
                    self.last_db_poll = now
                    
                # --- 3. Job Triggering (Every 10s) ---
                if now - self.last_job_trigger > 10:
                    self._trigger_jobs()
                    self.last_job_trigger = now
                    # Small sleep to prevent 100% CPU usage
                    time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def run_once(self, job_id: int) -> None:
        """Used for Dumb Trigger Mode (Airflow/CLI)"""
        print(f"Starting Dumb Task Mode for Job {job_id}...")
        job: Optional[Job] = self.db.get_job_by_id(job_id)
        
        if job:
            self._execute_logic(job)
            # We don't loop. We don't ping watchdog. We just finish.
            print("Task Complete. Exiting.")
        else:
            print("Error: Job ID not found.")
            sys.exit(1)

    def _execute_logic(self, job: Job) -> None:
        """The core ingestion code shared by BOTH modes"""
        # 1. Check Bitmask
        # 2. Run Ray Tasks
        # 3. Update DB
        pass

    def stop(self) -> None:
        """Graceful shutdown for Always-On"""
        print("Shutting down gracefully...")
        # Close DB connections, stop Ray actors, etc.
        sys.exit(0)
