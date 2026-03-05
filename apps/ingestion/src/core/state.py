import logging
import time
from typing import Any

from src.core.entities.job.manifest import JobManifest

LOG = logging.getLogger(__name__)
CURRENT_EXECUTION_TBL = "CURRENT_EXECUTION"

class StateStore:
    
    def __init__(self, sb_client: DatabaseClient) -> None:
        self.client = db_client # Database-specific logic here
        self._mirror: dict[str, dict[str, Any]] = {}  # {job_id: {record_data}}
        self._dirty_keys: set[str] = set() # Track what needs saving
        self.last_sync = 0

    def refresh(self) -> None:
        """
        Polls Postgres for active job definitions and schedules.
        Updates the local mirror with the latest from the DB.
        """
        sql = f"""
            SELECT * FROM {CURRENT_EXECUTION_TBL} W
            HERE STATUS IN ("RUNNING", "QUEUED")
        """
        records = self.client.fetch(sql)
        for r in records:
            # We don't overwrite local 'RUNNING' states with DB state 
            # unless the local state is empty (startup)
            if r["job_id"] not in self._mirror:
                self._mirror[r["job_id"]] = r
                
    def get_active_definitions(self) -> list[dict[str, Any]]:
        """Returns the current list of jobs for the Orchestrator to evaluate."""
        return list(self._mirror.values())

    def update_run(self, manifest: JobManifest) -> None:
        """
        Updates the mirror based on a job's progress.
        This is called by TerminalSteps or the Orchestrator.
        """
        job_id = manifest.job_id
        if job_id in self._mirror:
            self._mirror[job_id]["status"] = manifest.status
            self._mirror[job_id]["next_scheduled_time"] = manifest.next_scheduled_time
            self._dirty_keys.add(job_id)

    def skip_misfired_run(self, job_id: str) -> None:
        """Updates the next run time in the mirror to bypass a misfire."""
        # Logic to calculate the NEXT cron occurrence goes here
        # For now, we just push it forward
        if job_id in self._mirror:
            self._mirror[job_id]["next_scheduled_time"] = time.time() + 3600 
            self._dirty_keys.add(job_id)
            
    def sync_from_folder(self, folder_path: Path) -> None:
        """
        Reads the manifest.json from a physical folder and 
        syncs the internal state/database mirror.
        """
        manifest_file = folder_path / "manifest.json"
        
        if not manifest_file.exists():
            LOG.warning(f"No manifest found at {manifest_file}. Sync skipped.")
            return

        try:
            # 1. Fast decode using msgspec
            with manifest_file.open("rb") as f:
                manifest = msgspec.json.decode(f.read(), type=JobManifest)

            # 2. Update the internal mirror
            # We use the run_id as the primary key for the mirror
            run_id = manifest.run_id
            
            # Map manifest to the DB structure expected by your SQL
            self._mirror[run_id] = {
                "run_id": run_id,
                "job_id": manifest.job_id,
                "status": manifest.status,
                "step": manifest.current_step,
                "bitmask": manifest.bitmask,
                "folder_path": str(folder_path),
                "last_updated": time.time()
            }
            
            self._dirty_keys.add(run_id)
            LOG.debug(f"Synced {run_id} from disk: Status={manifest.status}")

        except Exception as e:
            LOG.error(f"Failed to sync manifest from {folder_path}: {e}")

    def flush(self) -> None:
        """Persists 'dirty' breadcrumbs to Postgres."""
        if not self._dirty_keys:
            return

        batch_data = [self._mirror[k] for k in self._dirty_keys]
        
        # Updated SQL to match your CURRENT_EXECUTIONS table
        sql = """
            INSERT INTO CURRENT_EXECUTIONS (
                RUN_ID, JOB_ID, JOB_STATUS, CURRENT_STEP, 
                JOB_BITMASK, FOLDER_PATH, LAST_UPDATED_AT_TS
            )
            VALUES (
                :run_id, :job_id, :status, :step, 
                :bitmask, :folder_path, NOW()
            )
            ON CONFLICT (RUN_ID) DO UPDATE SET 
                JOB_STATUS = EXCLUDED.JOB_STATUS, 
                CURRENT_STEP = EXCLUDED.CURRENT_STEP,
                JOB_BITMASK = EXCLUDED.JOB_BITMASK,
                FOLDER_PATH = EXCLUDED.FOLDER_PATH,
                LAST_UPDATED_AT_TS = NOW();
        """
        
        try:
            self.client.execute_batch(sql, batch_data)
            self._dirty_keys.clear()
            self.last_sync = time.time()
        except Exception as e:
            LOG.error(f"Postgres batch sync failed: {e}")
