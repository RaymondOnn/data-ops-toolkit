import time
from collections import ChainMap
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any
from zoneinfo import ZoneInfo

import msgspec
import polars as pl
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.contexts.task import TaskContext
from apps.ingestion.src.core.models.task import ExecutionStatus, TaskManifest
from apps.ingestion.src.core.orchestrator.enums import JobRecord
from apps.ingestion.src.services.database import DatabaseSink
from apps.ingestion.src.utils.constants import (
    CONFIG_FILENAME,
    MANIFEST_FILENAME,
    MISFIRE_GRACE_PERIOD_SECS,
)

LOG = logger
SOURCE_TBL = "META.CURRENT_EXECUTION"
DESTINATION_TBL = "META.EXECUTION_LOG"


def to_ch_datetime(ts: Any) -> datetime | None:
    """
    Converts various timestamp formats into a timezone-naive datetime object
    suitable for ClickHouse DateTime64 columns (which interpret naive values
    in their defined timezone).
    """
    if not ts:
        return None
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts)
        except ValueError:
            return None  # Return None if string cannot be parsed as datetime
    return ts.replace(tzinfo=None)


# TODO: Logging to Error Log? Workflow for refresh current_execution for the day
# TODO: Misfire Updates
class StateStore:
    def __init__(self, db_service: DatabaseSink, exec_ctx: ExecutionContext) -> None:
        self.db: DatabaseSink = db_service
        self.exec_ctx = exec_ctx
        self.workspace_dir = self.exec_ctx.state_path
        self.stage_dir = self.workspace_dir / "stage"
        self.archive_dir = self.workspace_dir / "archive"
        # Attribute to store the queried records (The Active Registry)
        self._active_records: dict[str, JobRecord] = {}
        self._column_cache: list[str] | None = None

        # Ensure directories exist
        for d in [self.stage_dir, self.archive_dir]:
            d.mkdir(parents=True, exist_ok=True)
        self.stream_path = self.workspace_dir / "execution_stream.jsonl"
        self.last_sync = 0
        self.last_flush = 0
        self._update_buffer: list[JobRecord] = []
        self._buffer_lock = Lock()
        self._registry_lock = Lock()

    def _create_updated_record(
        self, base_record: JobRecord | None, updates: dict[str, Any]
    ) -> JobRecord:
        """
        Creates a new JobRecord by merging updates into an existing record.
        Ensures LAST_UPDATED_AT_TS_LC is always current.
        """
        # Start with a base dictionary from the existing record, or an empty one
        base_dict = msgspec.to_builtins(base_record) if base_record else {}

        # Apply updates, ensuring LAST_UPDATED_AT_TS_LC is always current and naive
        merged_dict = dict(ChainMap(updates, base_dict))
        merged_dict["LAST_UPDATED_AT_TS_LC"] = to_ch_datetime(
            datetime.now(ZoneInfo(self.exec_ctx.timezone))
        )

        try:
            return msgspec.convert(merged_dict, JobRecord)
        except msgspec.ValidationError as e:
            LOG.error(
                "StateStore Validation Failure: Could not build updated JobRecord",
                error=str(e),
                base_record=base_dict,
                updates=updates,
            )
            raise

    @property
    def active_registry(self) -> dict[str, JobRecord]:
        if not self._active_records and self.exec_ctx.always_on:
            self.get_latest_state()
        with self._registry_lock:
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
        new_active_records = {}  # Build new registry locally
        if not self._active_records or force_refresh:
            LOG.debug("Refreshing active records from database view")

        # We only care about jobs that are in active states
        active_statuses = [f"'{s.value}'" for s in ExecutionStatus.active_statuses()]
        status_filter = ", ".join(active_statuses)

        # Debug: Check the ClickHouse "Now" to ensure no timezone drift
        tz = self.exec_ctx.timezone
        ch_now = self.db.fetch(f"SELECT now64(3, '{tz}')")[0][0]

        sql = f"""
            SELECT * FROM {SOURCE_TBL} 
            WHERE JOB_STATUS IN ({status_filter})
            AND RUN_ID IS NOT NULL
            AND SCHEDULED_TIMESTAMP_LC <= now64(3, '{tz}') 
            + INTERVAL {lookahead_mins} MINUTE
        """

        LOG.debug(
            "Querying active records",
            lookahead=lookahead_mins,
            ch_now=ch_now,
            status_filter=status_filter,
        )

        try:
            # Fetch column names dynamically from ClickHouse metadata to avoid hardcoding
            if not self._column_cache:
                self._column_cache = [
                    row[0] for row in self.db.fetch(f"DESCRIBE TABLE {SOURCE_TBL}")
                ]

            raw_records = self.db.fetch(sql)
            for r in raw_records:
                # Convert tuple results to dictionary for named access if necessary
                data = (
                    dict(zip(self._column_cache, r, strict=False))
                    if isinstance(r, (tuple, list))
                    else r
                )

                # ClickHouse FixedString(N) columns pad strings with null bytes
                # to fill the fixed length N;
                # we must decode to str for msgspec
                raw_dict = {
                    k: (v.decode("utf-8").rstrip("\x00") if isinstance(v, bytes) else v)
                    for k, v in data.items()
                }

                # Instantiate the structured model (Validation happens here)
                try:
                    record = msgspec.convert(raw_dict, JobRecord)
                except msgspec.ValidationError as e:
                    LOG.error(
                        "Validation failed for record in refresh",
                        error=str(e),
                        raw_data=raw_dict,
                    )
                    continue

                # Use RUN_ID as the definitive key for the Active Registry
                new_active_records[record.RUN_ID] = record

            with self._registry_lock:
                self._active_records = new_active_records  # Atomic swap

            if not raw_records:
                # Check if the table is actually empty or just filtered
                total_rows = self.db.fetch(f"SELECT count() FROM {SOURCE_TBL}")[0][0]
                LOG.debug(
                    "No active records found after filtering "
                    "(Total in table: {total_in_table})",
                    total_in_table=total_rows,
                    sql=sql,
                )

            LOG.info("Received {count} active records", count=len(raw_records))
        except Exception:
            LOG.exception("Failed to refresh active records")
            raise
            # Fallback to empty dict to avoid NoneType errors in Orchestrator loop
            with self._registry_lock:
                self._active_records = new_active_records or {}  # Ensure it's not None

        return self._active_records

    def resolve_task_path(self, identifier: str) -> Path | None:
        """Locates the physical directory for a task identifier or run_id."""
        from apps.ingestion.src.utils.common import find_path

        path = find_path(self.exec_ctx.workspace_dir, identifier)
        return path if path and path.exists() else None

    def create_record(
        self,
        job_id: str,
        dataset_id: str,
        partition_date: str,
        run_id: str,
        status: str = "PENDING",
    ) -> None:
        """Manually seeds the Active Registry for ad-hoc or dumb-mode runs."""
        if run_id in self._active_records:
            LOG.debug(
                "Run ID already exists in Active Registry; "
                "skipping manual creation to prevent overwrite.",
                run_id=run_id,
            )
            return

        # Use the internal helper to ensure consistent formatting and validation
        updates = {
            "JOB_ID": job_id,
            "DATASET_ID": dataset_id,
            "PARTITION_DATE": partition_date,
            "RUN_ID": run_id,
            "IS_SCHEDULED": 0,
            "SCHEDULED_TIMESTAMP_LC": to_ch_datetime(
                datetime.now(ZoneInfo(self.exec_ctx.timezone))
            ),
            "JOB_STATUS": status,  # This will be handled by to_ch_datetime
        }
        with self._registry_lock:
            self._active_records[run_id] = self._create_updated_record(None, updates)

    def _flush_buffer_to_stream(self) -> None:
        """
        Periodically flushes the in-memory update buffer to the execution stream.
        This reduces disk I/O during high signal volume.
        """
        with self._buffer_lock:
            if not self._update_buffer:
                return

            LOG.debug("Flushing state buffer to stream", count=len(self._update_buffer))
            with self.stream_path.open("ab") as f:
                for record in self._update_buffer:
                    line = msgspec.json.encode(msgspec.to_builtins(record)) + b"\n"
                    f.write(line)
            self._update_buffer.clear()

    def remove_record(self, run_id: str) -> None:
        """Evicts a record from the in-memory active registry."""
        with self._registry_lock:
            if run_id and run_id in self._active_records:
                self._active_records.pop(run_id)
                LOG.debug("Evicted task from registry", run_id=run_id)

    def emit_expiry(
        self, run: JobRecord, context: TaskContext | None, reason: str
    ) -> None:
        """
        Specialized emission for expired runs that may not have physical manifests.
        """
        # 1. Resolve Context (Hydrate placeholder if missing)
        ctx = context or TaskContext.create_placeholder(run)

        # 2. Resolve Manifest (Try to rehydrate if dispatched,
        # otherwise create synthetic)
        manifest = None
        path = self.resolve_task_path(run.RUN_ID)
        if path and (path / MANIFEST_FILENAME).exists():
            try:
                with (path / MANIFEST_FILENAME).open("rb") as f:
                    manifest = msgspec.json.decode(f.read(), type=TaskManifest)
            except Exception:
                pass

        if not manifest:
            manifest = TaskManifest(
                job_id=run.JOB_ID,
                run_id=run.RUN_ID,
                dataset_id=run.DATASET_ID,
                status=ExecutionStatus.EXPIRED,
                current_stage="N.A",
                remarks=reason,
                bitmask=0,
            )

        self.emit_state(
            manifest=manifest,
            context=ctx,
            metadata={"status": ExecutionStatus.EXPIRED, "remarks": reason},
        )

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
        # Ensure all in-memory updates are written to the stream before processing
        self._flush_buffer_to_stream()

        if not self.stream_path.exists() or self.stream_path.stat().st_size == 0:
            return

        batch_id = int(time.time())
        temp_jsonl = self.stage_dir / f"batch_{batch_id}.jsonl"
        target_parquet = temp_jsonl.with_suffix(".parquet")

        try:
            # 1. Rotate & Convert
            self.stream_path.rename(temp_jsonl)
            pl.scan_ndjson(temp_jsonl).sink_parquet(target_parquet)
            rows_flushed = (
                pl.scan_parquet(target_parquet).select(pl.len()).collect().item()
            )
            temp_jsonl.unlink()  # JSONL is no longer needed once Parquet is cut

            # 2. Synchronously process the stage folder
            self._process_stage()

            self.last_flush = time.time()
            LOG.info("StateStore flush successful", batch=batch_id, rows=rows_flushed)

        except Exception as e:
            LOG.error("StateStore flush failed", error=str(e))
            # Critical: If rename happened but load failed,
            # we keep the file for manual recovery.

    def _process_stage(self):
        """Iterates through stage folder and moves successful loads to archive"""
        try:
            pending_files = list(self.stage_dir.glob("*.parquet"))
            if not pending_files:
                return

            # Load all pending Parquet files into the DB
            self.db.client.copy_from_file(
                table=DESTINATION_TBL,
                source_dir=str(self.stage_dir),
                file_ext="parquet",
            )
            for pq_file in pending_files:
                # Move to archive only on success
                pq_file.rename(self.archive_dir / pq_file.name)
                LOG.info("Successfully loaded and archived state", file=pq_file.name)
        except Exception:
            LOG.exception("Failed to load state files, leaving in stage for retry")
            raise

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

        # Get the current record from the registry, if it exists
        current_record = self._active_records.get(manifest.run_id)

        if current_record is None:
            LOG.warning(
                "Registry Lookup Miss: No existing record found for run",
                run_id=manifest.run_id,
            )

        # 1. Define the incoming updates based on manifest, context, and metadata
        status = str(metadata.get("status", manifest.status.value)).upper()

        # Map manifest/context attributes to DB columns.
        # We only define fields that have changed or are derived from files.
        incoming_update = {
            "RUN_ID": manifest.run_id,  # type: ignore
            "JOB_ID": manifest.job_id,  # type: ignore
            "DATASET_ID": manifest.dataset_id,  # type: ignore
            "PARTITION_DATE": context.partition_date,  # type: ignore
            "JOB_STATUS": status,  # type: ignore
            "CURRENT_STEP": manifest.current_stage.upper(),  # type: ignore
            "JOB_BITMASK": manifest.bitmask,  # type: ignore
            "RETRY_ATTEMPTS": manifest.retry_count,  # type: ignore
            "START_TIMESTAMP_LC": to_ch_datetime(
                getattr(manifest.start, "start_timestamp_utc", None)
            ),
            "END_TIMESTAMP_LC": to_ch_datetime(
                getattr(manifest.complete, "end_timestamp_utc", None)
            ),
            "SOURCE_ROW_COUNT": getattr(manifest.extract, "source_row_count", None),  # type: ignore
            "FINAL_ROW_COUNT": getattr(manifest.publish, "final_count", None),  # type: ignore
            "RUNTIME_OVERRIDES": context.custom_overrides or None,
            "FINAL_MANIFEST": msgspec.to_builtins(manifest) if deep_sync else None,
            # Fallback values for fields that might not be in manifest/context but are in JobRecord
            "SCHEDULED_TIMESTAMP_LC": to_ch_datetime(
                getattr(
                    current_record,
                    "SCHEDULED_TIMESTAMP_LC",
                    datetime.now(ZoneInfo(self.exec_ctx.timezone)),
                )
            ),
            "IS_SCHEDULED": getattr(current_record, "IS_SCHEDULED", 0),  # type: ignore
            "WATCH_FILE_PATH": getattr(current_record, "WATCH_FILE_PATH", None),  # type: ignore
            "IS_SNAPSHOT": getattr(current_record, "IS_SNAPSHOT", False),  # type: ignore
            "EXPIRATION_THRESHOLD": to_ch_datetime(
                getattr(current_record, "EXPIRATION_THRESHOLD", None)
            ),  # type: ignore
            "MISFIRE_GRACE_SECS": getattr(current_record, "MISFIRE_GRACE_SECS", MISFIRE_GRACE_PERIOD_SECS),  # type: ignore
            "TRIGGER_TYPE": getattr(current_record, "TRIGGER_TYPE", "CRON"),  # type: ignore
        }

        # Merge with metadata overrides (e.g. remarks)
        incoming_update.update(metadata)

        # Delegate to the shared engine
        self.update_run(manifest.run_id, incoming_update)

    def update_run(self, run_id: str, updates: dict[str, Any]) -> None:
        """
        Applies partial updates to a tracked run and emits the new state.
        Perfect for heartbeats (.sync) or lightweight status changes (.retry).
        """
        current = self._active_records.get(run_id)
        if not current:
            LOG.warning("Update failed: Run ID not found in registry", run_id=run_id)
            return

        # Generate the new record outside the lock, then update atomically
        new_record = self._create_updated_record(current, updates)

        # Update the in-memory registry immediately
        with self._registry_lock:
            self._active_records[run_id] = new_record

        # Add to buffer for batched writing to disk
        with self._buffer_lock:
            self._update_buffer.append(new_record)
