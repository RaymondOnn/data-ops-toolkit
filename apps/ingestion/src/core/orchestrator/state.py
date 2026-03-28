import time
from collections import ChainMap
from datetime import datetime
from pathlib import Path
from typing import Any

import msgspec
import polars as pl
import structlog

from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.contexts.job import TaskContext
from apps.ingestion.src.core.models.job import ExecutionStatus, TaskManifest
from apps.ingestion.src.services.database import DatabaseSink
from apps.ingestion.src.utils.constants import ALWAYS_ON_MODE

LOG = structlog.getLogger(__name__)
CURRENT_EXECUTION_TBL = "CURRENT_EXECUTION"


# TODO: Logging to Error Log? Workflow for refresh current_execution for the day
# TODO: Misfire Updates
class StateStore:
    def __init__(self, db_service: DatabaseSink, exec_ctx: ExecutionContext) -> None:
        self.db: DatabaseSink = db_service
        self.exec_ctx = exec_ctx
        self.workspace_dir = self.exec_ctx.state_path
        self.stage_dir = self.workspace_dir / "stage"
        self.archive_dir = self.workspace_dir / "archive"
        # Attribute to store the queried records (The Hot Cache)
        self._active_records: dict[str, dict[str, Any]] = {}

        # Ensure directories exist
        for d in [self.stage_dir, self.archive_dir]:
            d.mkdir(parents=True, exist_ok=True)
        self.stream_path = self.workspace_dir / "execution_stream.jsonl"
        self.last_sync = 0
        self.last_flush = 0

    @property
    def active_records(self) -> dict[str, dict[str, Any]]:
        if not self._active_records and ALWAYS_ON_MODE:
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
        active_statuses = [f"'{s.value}'" for s in ExecutionStatus.active_statuses()]
        status_filter = ", ".join(active_statuses)

        sql = f"""
            SELECT * FROM {CURRENT_EXECUTION_TBL} 
            WHERE JOB_STATUS IN ({status_filter})
        """
        try:
            raw_records = self.db.fetch(sql)
            for r in raw_records:
                identifier = self.exec_ctx.get_task_identifier(
                    r["JOB_ID"], r["DATASET_ID"], str(r["RUN_DATE"])
                )
                self._active_records[identifier] = r
        except Exception as e:
            LOG.error("Failed to refresh active records", error=str(e))
            # Fallback to empty dict to avoid NoneType errors in Orchestrator loop
            self._active_records = self._active_records or {}

        return self._active_records

    def create_record(self, job_id: str, dataset_id: str, run_date: str):
        from apps.ingestion.src.utils.common import find_path

        # Use existing find_path utility to locate the directory anywhere in the workspace
        identifier = self.exec_ctx.get_task_identifier(job_id, dataset_id, run_date)
        active_path = find_path(self.exec_ctx.workspace_dir, identifier)

        if active_path and active_path.exists():
            # Minimal record to satisfy the sync requirements
            self.active_records[identifier] = {
                "JOB_ID": job_id,
                "DATASET_ID": dataset_id,
                "RUN_DATE": run_date,
            }

    # TODO:
    def update_status(self, job_id: str, status: str) -> None:
        """
        Updates the status of a job in the database.
        """
        LOG.info("Updating job status", job_id=job_id, status=status)
        # In a real implementation, you would emit this to the stream
        # or execute a direct DB update here.
        # For now, we log it to ensure observability of the intent.

    # TODO:
    def update_run(self, run_id: str, updates: dict[str, Any]) -> None:
        """
        Updates a specific run's metadata.
        """
        LOG.info("Updating run state", run_id=run_id, updates=updates)
        # This serves as a hook for the LifecycleManager to flag expired runs.

    # TODO:
    def skip_misfired_run(self, job_id: str) -> None:
        LOG.warning("Skipping misfired run", job_id=job_id)
        # Update DB next_run_time logic would go here

    def sync_from_folder(self, folder_path: Path, deep_sync: bool = False) -> None:
        """
        Reads the manifest.json from a physical folder and
        syncs the internal state/database mirror.
        """
        manifest_file = folder_path / "manifest.json"
        config_file = folder_path / "config.json"

        if not manifest_file.exists():
            LOG.warning("No manifest found. Sync skipped.", path=str(manifest_file))
            return

        if not config_file.exists():
            LOG.warning("No config found. Sync skipped.", path=str(config_file))
            return

        try:
            # 1. Fast decode using msgspec
            with manifest_file.open("rb") as f:
                manifest = msgspec.json.decode(f.read(), type=TaskManifest)

            with config_file.open("rb") as f:
                ctx = msgspec.yaml.decode(f.read(), type=TaskContext)

            # 2. Emit to the local stream immediately
            # We flag this as a 'SYNC' event in metadata if needed
            self.emit_state(
                manifest=manifest,
                context=ctx,
                deep_sync=deep_sync,
            )

            LOG.debug(
                "Synced manifest from disk",
                run_id=manifest.run_id,
                status=manifest.status,
            )

        except (msgspec.DecodeError, msgspec.ValidationError) as e:
            LOG.error("Malformed manifest", path=str(folder_path), error=str(e))
        except OSError as e:
            LOG.error(
                "FileSystem error syncing manifest", path=str(folder_path), error=str(e)
            )
        except Exception:
            LOG.exception("Unexpected error syncing manifest", path=str(folder_path))

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
                "Failed to load state files, leaving in stage for retry",
                error=str(e),
            )

    def emit_state(
        self,
        manifest: TaskManifest | None = None,
        context: TaskContext | None = None,
        metadata: dict[str, Any] | None = None,
        deep_sync: bool = False,
    ) -> None:
        """
        Appends state to the local JSONL stream.
        Low latency, disk-persistent.
        """
        metadata = metadata or {}
        identifier = self.exec_ctx.get_task_identifier(
            manifest.job_id, manifest.dataset_id, context.run_date
        )
        record = self.active_records.get(identifier, {})

        # Align keys with your execution_log.sql columns
        incoming_update = {
            "RUN_ID": manifest.run_id if manifest else None,
            "JOB_ID": manifest.job_id if manifest else None,
            "SCHEDULED_TIMESTAMP": record.get("SCHEDULED_TIMESTAMP", None),
            "DATASET_ID": manifest.dataset_id if manifest else None,
            "RUN_DATE": context.run_date if context else None,
            "START_TIMESTAMP": (
                manifest.start.start_timestamp_utc if manifest.start else None
            ),
            "END_TIMESTAMP": (
                manifest.complete.end_timestamp_utc if manifest.complete else None
            ),
            "LAST_UPDATED_AT_TS": datetime.now().astimezone().isoformat(),
            "JOB_STATUS": str(metadata.get("JOB_STATUS", manifest.status)).upper(),
            "CURRENT_STEP": manifest.current_stage.upper(),
            "JOB_BITMASK": manifest.bitmask,
            "WATCH_FILE_PATH": record.get("WATCH_FILE_PATH", None),
            "RUNTIME_OVERRIDES": context.custom_overrides,
            "RETRY_ATTEMPTS": manifest.retry_count,
            "SOURCE_ROW_COUNT": (
                manifest.extract.source_row_count if manifest.extract else None
            ),
            "FINAL_ROW_COUNT": (
                manifest.publish.final_count if manifest.publish else None
            ),
            "FINAL_MANIFEST": msgspec.json.encode(manifest) if deep_sync else None,
        }

        # 2. Chain them: current_transition takes priority, record is the fallback
        # We use .copy() at the end to turn it back into a plain dict for
        # JSON serialization
        event = dict(ChainMap(incoming_update, record))

        # 3. Update the Hot Cache so the next call sees the combined state
        self._active_records[identifier] = event

        line = msgspec.json.encode(event) + b"\n"
        with self.stream_path.open("ab") as f:
            f.write(line)
            f.write(line)
            f.write(line)
            f.write(line)
            f.write(line)
            f.write(line)
            f.write(line)
            f.write(line)
            f.write(line)
