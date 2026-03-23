import time
from datetime import datetime
from pathlib import Path
from typing import Any

import msgspec
import polars as pl
import structlog
from src.core.contexts.execution import ExecutionContext
from src.core.models.job import JobManifest, JobStatus
from src.services.database import DatabaseSink

from core.contexts.job import JobContext

LOG = structlog.getLogger(__name__)
CURRENT_EXECUTION_TBL = "CURRENT_EXECUTION"


# TODO: Logging to Error Log? Workflow for refresh current_execution for the day
class StateStore:
    def __init__(self, db_service: DatabaseSink, exec_ctx: ExecutionContext) -> None:
        self.db: DatabaseSink = db_service
        self.workspace_dir = Path(exec_ctx.workspace_dir) / "state"
        self.stage_dir = self.workspace_dir / "stage"
        self.archive_dir = self.workspace_dir / "archive"
        # Attribute to store the queried records (The Hot Cache)
        self._active_records: dict[str, dict[str, Any]] = {}

        # Ensure directories exist
        for d in [self.stage_dir, self.archive_dir]:
            d.mkdir(parents=True, exist_ok=True)
        self.stream_path = self.workspace_dir / "execution_stream.jsonl"
        self.last_sync = 0

    @property
    def active_records(self) -> dict[str, dict[str, Any]]:
        if self._active_records is None:
            self.get_latest_state()
        return self._active_records

    def get_latest_state(
        self, force_refresh: bool = False
    ) -> dict[str, dict[str, Any]]:
        """
        Returns the map of active/pending/blocked/deferred records.
        If _active_records is None or force_refresh is True, it queries the DB view.
        """
        if self._active_records is None or force_refresh:
            LOG.info("Refreshing active records from database view")

        # We only care about jobs that are not SUCCESS, FAILED, or EXPIRED
        active_statuses = [f"'{s.value}'" for s in JobStatus.active_statuses()]
        status_filter = ", ".join(active_statuses)

        sql = f"""
            SELECT * FROM {CURRENT_EXECUTION_TBL} 
            WHERE JOB_STATUS IN ({status_filter})
        """
        try:
            raw_records = self.db.fetch(sql)
            self._active_records = {r["RUN_ID"]: r for r in raw_records}
        except Exception as e:
            LOG.error("Failed to refresh active records", error=str(e))
            # Fallback to empty dict to avoid NoneType errors in Orchestrator loop
            self._active_records = self._active_records or {}

        return self._active_records

    def sync_from_folder(self, folder_path: Path, deep_sync: bool = False) -> None:
        """
        Reads the manifest.json from a physical folder and
        syncs the internal state/database mirror.
        """
        manifest_file = folder_path / "manifest.json"
        config_file = folder_path / "config.json"

        if not manifest_file.exists():
            LOG.warning(f"No manifest found at {manifest_file}. Sync skipped.")
            return

        if not config_file.exists():
            LOG.warning(f"No config found at {config_file}. Sync skipped.")
            return

        try:
            # 1. Fast decode using msgspec
            with manifest_file.open("rb") as f:
                manifest = msgspec.json.decode(f.read(), type=JobManifest)

            with config_file.open("rb") as f:
                ctx = msgspec.yaml.decode(f.read(), type=JobContext)

            # 2. Emit to the local stream immediately
            # We flag this as a 'SYNC' event in metadata if needed
            self.emit_state(
                manifest=manifest, context=ctx,  metadata={"sync_source": "disk_recovery"}, deep_sync=deep_sync
            )

            LOG.debug(
                f"Synced {manifest.run_id} from disk: Status={manifest.job_status}"
            )

        except (msgspec.DecodeError, msgspec.ValidationError) as e:
            LOG.error(f"Malformed manifest at {folder_path}: {e}")
        except OSError as e:
            LOG.error(f"FileSystem error syncing manifest from {folder_path}: {e}")
        except Exception:
            LOG.exception(f"Unexpected error syncing manifest from {folder_path}")

    def _calculate_bitmask(self, job_path: Path) -> int:
        """Simple logic to check which active links exist."""
        mask = 0
        if (job_path / "extract").exists():
            mask |= 1
        if (job_path / "transform").exists():
            mask |= 2
        if (job_path / "write").exists():
            mask |= 4
        if (job_path / "publish").exists():
            mask |= 8
        return mask

    def flush(self) -> None:
        """
        Rotates JSONL to Parquet and loads into Database.
        """
        if not self.stream_path.exists() or self.stream_path.stat().st_size == 0:
            return

        batch_id = int(time.time())
        temp_jsonl = self.stage_dir / f"batch_{batch_id}.jsonl"
        target_parquet = temp_jsonl.with_suffix(".parquet")

        try:
            # 1. Rotate & Convert
            self.stream_path.rename(temp_jsonl)
            df = pl.read_ndjson(temp_jsonl)
            df.write_parquet(target_parquet)
            temp_jsonl.unlink()  # JSONL is no longer needed once Parquet is cut

            # 2. Attempt Load for ALL files in stage (Retrying old failures)
            self.db.stage_data(self.stage_dir, "EXECUTION_LOG")

            self.last_flush = time.time()
            LOG.info("StateStore flush successful", batch=batch_id, rows=df.height)

        except Exception as e:
            LOG.error("StateStore flush failed", error=str(e))
            # Critical: If rename happened but load failed,
            # we keep the file for manual recovery.

    def _process_stage(self):
        """Iterates through stage folder and moves successful loads to archive"""
        try:
            self.db.stage_data(self.stage_dir, "EXECUTION_LOG", "parquet")
            for pq_file in self.stage_dir.glob("*.parquet"):
                # Move to archive only on success
                pq_file.rename(self.archive_dir / pq_file.name)
                LOG.info("Successfully loaded and archived state", file=pq_file.name)
        except Exception as e:
            LOG.warning(
                "Failed to load state file, leaving in stage for retry",
                file=pq_file.name,
                error=str(e),
            )

    def emit_state(
        self,
        manifest: JobManifest,
        context: JobContext,
        deep_sync: bool = False,
        metadata: dict | None = None,
    ) -> None:
        """
        Appends state to the local JSONL stream.
        Low latency, disk-persistent.
        """

        metadata = metadata or {}
        record = self.active_records.get(manifest.run_id, {})

        # Align keys with your execution_log.sql columns
        event = {
            "RUN_ID": manifest.run_id,
            "JOB_ID": manifest.job_id,
            "SCHEDULED_TIMESTAMP": record.get("SCHEDULED_TIMESTAMP", None),
            "DATASET_ID": manifest.dataset_id,
            "RUN_DATE": context.run_date,
            "START_TIMESTAMP": manifest.start.start_timestamp_utc
            if manifest.start
            else None,
            "END_TIMESTAMP": manifest.complete.end_timestamp_utc
            if manifest.complete
            else None,
            "LAST_UPDATED_AT_TS": datetime.now().astimezone().isoformat(),
            "JOB_STATUS": str(manifest.job_status).upper(),
            "CURRENT_STEP": manifest.current_step.upper(),
            "JOB_BITMASK": manifest.bitmask,
            "WATCH_FILE_PATH": record.get("WATCH_FILE_PATH", None),
            "RUNTIME_OVERRIDES": context.custom_overrides,
            "RETRY_ATTEMPTS": manifest.retry_count,
            "SOURCE_ROW_COUNT": manifest.extract.source_row_count
            if manifest.extract
            else None,
            "FINAL_ROW_COUNT": manifest.publish.final_count
            if manifest.publish
            else None,
            "FINAL_MANIFEST": msgspec.json.encode(manifest) if deep_sync else None,
        }

        line = msgspec.json.encode(event) + b"\n"
        with self.stream_path.open("ab") as f:
            f.write(line)
