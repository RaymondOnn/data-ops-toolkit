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
                # 1. Look for the 'Two Earliest' jobs in DB
                jobs: List[Optional[Job]] = self.db.get_next_queued_jobs(limit=2)
                for job in jobs:
                    if job:
                        self._execute_logic(job)
                
                # 2. Heartbeat for Systemd
                self.heartbeat.ping()
                time.sleep(60)
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
