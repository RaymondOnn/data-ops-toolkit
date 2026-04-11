import time
from collections import ChainMap
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import msgspec
import polars as pl
import structlog
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.contexts.job import TaskContext
from apps.ingestion.src.core.models.job import ExecutionStatus, TaskManifest
from apps.ingestion.src.core.orchestrator.enums import JobRecord
from apps.ingestion.src.services.database import DatabaseSink
from apps.ingestion.src.utils.constants import CONFIG_FILENAME, MANIFEST_FILENAME
from tenacity import retry, stop_after_attempt, wait_exponential

LOG = structlog.getLogger(__name__)
SOURCE_TBL = "META.CURRENT_EXECUTION"
DESTINATION_TBL = "META.EXECUTION_LOG"


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
        self._active_records: dict[str, JobRecord] = {}

        # Ensure directories exist
        for d in [self.stage_dir, self.archive_dir]:
            d.mkdir(parents=True, exist_ok=True)
        self.stream_path = self.workspace_dir / "execution_stream.jsonl"
        self.last_sync = 0
        self.last_flush = 0

    @property
    def active_records(self) -> dict[str, JobRecord]:
        if not self._active_records and self.exec_ctx.always_on:
            self.get_latest_state()
        return self._active_records

    def get_latest_state(
        self, force_refresh: bool = False, lookahead_mins: int = 60
    ) -> dict[str, JobRecord]:
        """
        Returns the map of active/pending/blocked/deferred records.
        If _active_records is None or force_refresh is True, it queries the DB view.

        Args:
            lookahead_mins: Filter jobs scheduled within the next X minutes.
        """
        if not self._active_records or force_refresh:
            LOG.debug("Refreshing active records from database view")

        # We only care about jobs that are not SUCCESS, FAILED, or EXPIRED
        active_statuses = [f"'{s.value}'" for s in ExecutionStatus.active_statuses()]
        status_filter = ", ".join(active_statuses)

        # Debug: Check the ClickHouse "Now" to ensure no timezone drift
        tz = self.exec_ctx.timezone
        ch_now = self.db.fetch(f"SELECT now64(3, '{tz}')")[0][0]

        sql = f"""
            SELECT * FROM {SOURCE_TBL} 
            WHERE JOB_STATUS IN ({status_filter})
            AND SCHEDULED_TIMESTAMP <= now64(3, '{tz}') + INTERVAL {lookahead_mins} MINUTE
        """

        LOG.debug(
            "Querying active records",
            lookahead=lookahead_mins,
            ch_now=ch_now,
            status_filter=status_filter,
        )

        try:
            # Fetch column names dynamically from ClickHouse metadata to avoid hardcoding
            cols = [
                row[0]
                for row in self.db.fetch(f"DESCRIBE TABLE {SOURCE_TBL}")
            ]

            raw_records = self.db.fetch(sql)
            for r in raw_records:
                # Convert tuple results to dictionary for named access if necessary
                raw_dict = dict(zip(cols, r)) if isinstance(r, (tuple, list)) else r

                # Instantiate the structured model (Validation happens here)
                record = msgspec.convert(raw_dict, JobRecord)

                identifier = self.exec_ctx.get_task_identifier(
                    record.JOB_ID, record.DATASET_ID, record.PARTITION_DATE or ""
                )
                self._active_records[identifier] = record

            if not raw_records:
                # Check if the table is actually empty or just filtered
                total_rows = self.db.fetch(
                    f"SELECT count() FROM {SOURCE_TBL}"
                )[0][0]
                LOG.warning(
                    "No active records found after filtering",
                    total_in_table=total_rows,
                    sql=sql,
                )

            LOG.info("Successfully refreshed active records", count=len(raw_records))
        except Exception as e:
            LOG.error("Failed to refresh active records", error=str(e))
            # Fallback to empty dict to avoid NoneType errors in Orchestrator loop
            self._active_records = self._active_records or {}

        return self._active_records

    def resolve_task_path(self, identifier: str) -> Path | None:
        """Locates the physical directory for a task identifier or run_id."""
        from apps.ingestion.src.utils.common import find_path

        path = find_path(self.exec_ctx.workspace_dir, identifier)
        return path if path and path.exists() else None

    def create_record(self, job_id: str, dataset_id: str, partition_date: str) -> None:
        identifier = self.exec_ctx.get_task_identifier(
            job_id, dataset_id, partition_date
        )
        task_path = self.resolve_task_path(identifier)

        if task_path:
            # Minimal record to satisfy the sync requirements
            self.active_records[identifier] = JobRecord(
                JOB_ID=job_id,
                DATASET_ID=dataset_id,
                PARTITION_DATE=partition_date,
                IS_SCHEDULED=0,
                SCHEDULED_TIMESTAMP=datetime.now(ZoneInfo(self.exec_ctx.timezone)),
                JOB_STATUS="PENDING",
            )

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

    def sync_from_folder(
        self, folder_path: Path | str, deep_sync: bool = False
    ) -> None:
        """
        Reads the manifest.json from a physical folder and
        syncs the internal state/database mirror.
        """
        # Handle case where a run_id or identifier is passed instead of a full path
        if isinstance(folder_path, str) and not Path(folder_path).exists():
            resolved = self.resolve_task_path(folder_path)
            if not resolved:
                LOG.warning(
                    "Sync failed: path could not be resolved from identifier",
                    path=folder_path,
                )
                return
            folder_path = resolved

        folder_path = Path(folder_path)

        manifest_file = folder_path / MANIFEST_FILENAME
        config_file = folder_path / CONFIG_FILENAME

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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
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

            if df.is_empty():
                return

            df.write_parquet(target_parquet)
            temp_jsonl.unlink()  # JSONL is no longer needed once Parquet is cut

            # 2. Synchronously process the stage folder
            self._process_stage()

            self.last_flush = time.time()
            LOG.info("StateStore flush successful", batch=batch_id, rows=df.height)

        except Exception as e:
            LOG.error("StateStore flush failed", error=str(e))
            # Critical: If rename happened but load failed,
            # we keep the file for manual recovery.

    def _process_stage(self):
        """Iterates through stage folder and moves successful loads to archive"""
        try:
            # Load all pending Parquet files into the DB
            self.db.stage_data(self.stage_dir, "META.EXECUTION_LOG")
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
        if manifest is None or context is None:
            LOG.warning("Cannot emit state without manifest and context")
            return

        metadata = metadata or {}
        identifier = self.exec_ctx.get_task_identifier(
            manifest.job_id, manifest.dataset_id, context.partition_date
        )
        record = self.active_records.get(identifier)
        record_dict = msgspec.to_builtins(record) if record else {}

        # Align keys with your execution_log.sql columns
        incoming_update = {
            "RUN_ID": manifest.run_id,
            "JOB_ID": manifest.job_id,
            "SCHEDULED_TIMESTAMP": record.SCHEDULED_TIMESTAMP if record else None,
            "DATASET_ID": manifest.dataset_id,
            "PARTITION_DATE": context.partition_date,
            "START_TIMESTAMP": (
                manifest.start.start_timestamp_utc if manifest.start else None
            ),
            "END_TIMESTAMP": (
                manifest.complete.end_timestamp_utc if manifest.complete else None
            ),
            "LAST_UPDATED_AT_TS": datetime.now(
                ZoneInfo(self.exec_ctx.timezone)
            ).isoformat(sep=" ", timespec="milliseconds"),
            "JOB_STATUS": str(metadata.get("status", manifest.status.value)).upper(),
            "CURRENT_STEP": manifest.current_stage.upper(),
            "JOB_BITMASK": manifest.bitmask,
            "IS_SCHEDULED": record.IS_SCHEDULED if record else 0,
            "WATCH_FILE_PATH": record.WATCH_FILE_PATH if record else None,
            "RUNTIME_OVERRIDES": (
                msgspec.json.encode(context.custom_overrides).decode()
                if context.custom_overrides
                else None
            ),
            "RETRY_ATTEMPTS": manifest.retry_count,
            "SOURCE_ROW_COUNT": (
                manifest.extract.source_row_count if manifest.extract else None
            ),
            "FINAL_ROW_COUNT": (
                manifest.publish.final_count if manifest.publish else None
            ),
            "FINAL_MANIFEST": (
                msgspec.json.encode(manifest).decode() if deep_sync else None
            ),
        }

        # 2. Chain them: current_transition takes priority, record is the fallback
        # We use .copy() at the end to turn it back into a plain dict for
        # JSON serialization

        event = dict(ChainMap(incoming_update, record_dict))

        # 3. Update the Hot Cache so the next call sees the combined state
        self._active_records[identifier] = msgspec.convert(event, JobRecord)

        line = msgspec.json.encode(event) + b"\n"
        with self.stream_path.open("ab") as f:
            f.write(line)
