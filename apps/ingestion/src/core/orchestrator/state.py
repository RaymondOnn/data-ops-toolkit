import time
from collections import ChainMap
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any

import msgspec
import polars as pl
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.contexts.task import TaskContext
from apps.ingestion.src.core.models.task import ExecutionStatus, TaskManifest
from apps.ingestion.src.core.orchestrator.enums import (
    JobRecord,
    JobUpdate,
    to_ch_datetime,
)
from apps.ingestion.src.services.database import DatabaseSink
from apps.ingestion.src.utils.constants import (
    CONFIG_FILENAME,
    MANIFEST_FILENAME,
    STRIP_TZ_FOR_DB,
)
from libs.database import TypeResolver
from libs.utils.dates import get_current_timestamp
from loguru import logger

LOG = logger
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
        # Attribute to store the queried records (The Active Registry)
        self._active_records: dict[str, JobRecord] = {}
        self._src_column_cache: list[str] | None = None
        self._dest_schema_cache: pl.DataFrame | None = None

        # Ensure directories exist
        for d in [self.stage_dir, self.archive_dir]:
            d.mkdir(parents=True, exist_ok=True)
        self.stream_path = self.workspace_dir / "execution_stream.jsonl"
        self.last_sync = 0
        self.last_flush = 0
        self._update_buffer: list[JobRecord] = []
        self._buffer_lock = Lock()
        self._registry_lock = Lock()

    def _get_target_schema(self) -> pl.DataFrame:
        """Retrieves and caches the schema for the log table."""
        if self._dest_schema_cache is None:
            # DESTINATION_TBL = "META.EXECUTION_LOG"
            # get_schema returns a DataFrame with column_name and data_type
            self._dest_schema_cache = self.db.client.get_schema(DESTINATION_TBL)
        return self._dest_schema_cache

    def _create_updated_record(
        self, base_record: JobRecord | None, updates: dict[str, Any]
    ) -> JobRecord:
        # 1. Convert base record to a mutable dictionary if it exists
        base_dict = msgspec.to_builtins(base_record) if base_record else {}

        # 2. Use ChainMap to merge. updates take precedence over base_dict.
        # We include default values for critical columns to ensure they are never null
        # if this is the first time a record is being created.
        merged = dict(ChainMap(updates, base_dict))

        # 3. FORCE the update of the tracking timestamp.
        # This ensures that even if 'updates' contains an old TS,
        # the orchestrator's current time wins.
        current_ts = get_current_timestamp(
            timezone=self.exec_ctx.timezone, strip_tz=STRIP_TZ_FOR_DB
        )
        merged["LAST_UPDATED_AT_TS_LC"] = to_ch_datetime(current_ts)

        # 4. Data Sanitization & Normalization
        # Ensure columns that might be missing in a partial update have safe defaults
        # for ClickHouse/Polars schema inference.
        merged.setdefault("JOB_BITMASK", 0)
        merged.setdefault("RETRY_ATTEMPTS", 0)
        merged.setdefault("IS_SCHEDULED", 0)

        # Ensure types are consistent (e.g., Bitmask should be int or str consistently)
        if merged.get("JOB_BITMASK") is not None:
            merged["JOB_BITMASK"] = int(merged["JOB_BITMASK"])

        # 5. Handle potential nested structures (like FINAL_MANIFEST)
        # If the update didn't provide a manifest, keep the old one
        if "FINAL_MANIFEST" not in updates and base_dict.get("FINAL_MANIFEST"):
            merged["FINAL_MANIFEST"] = base_dict["FINAL_MANIFEST"]

        # 6. Final conversion back to the JobRecord struct.
        # msgspec.convert validates that the merged dict matches your JobRecord definition.
        try:
            return msgspec.convert(merged, type=JobRecord)
        except Exception as e:
            LOG.error(
                "Failed to serialize JobRecord during merge",
                error=str(e),
                run_id=merged.get("RUN_ID"),
            )
            # Fallback to the base record if merge fails to prevent losing the track
            if base_record:
                return base_record

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
        ch_now = self.db.fetch("SELECT now64(3) + INTERVAL 8 HOUR AS NOW_TS_LC")[0][0]

        sql = f"""
             WITH dates AS (
                SELECT
                    8 AS OFFSET_HOURS
                    , now64(3) + INTERVAL OFFSET_HOURS HOUR AS NOW_TS_LC
                    , toDate(NOW_TS_LC) AS TODAY_LC
                    , toStartOfDay(NOW_TS_LC) AS TODAY_START_LC
            ) 
            SELECT * FROM {SOURCE_TBL} 
            WHERE JOB_STATUS IN ({status_filter})
            AND RUN_ID IS NOT NULL
            AND SCHEDULED_TIMESTAMP_LC <= (SELECT NOW_TS_LC FROM dates) 
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
            if not self._src_column_cache:
                self._src_column_cache = [
                    row[0] for row in self.db.fetch(f"DESCRIBE TABLE {SOURCE_TBL}")
                ]

            raw_records = self.db.fetch(sql)
            for r in raw_records:
                # 1. Map raw tuple to dict using cached columns
                raw_dict = dict(zip(self._src_column_cache, r, strict=False))

                # 2. Type Coercion for msgspec
                # ClickHouse returns bytes for fixed strings and datetime objects for timestamps.
                # We must convert these to strings to satisfy the JobRecord schema.
                for k, v in raw_dict.items():
                    if isinstance(v, bytes):
                        raw_dict[k] = v.decode("utf-8").rstrip("\x00")
                    elif isinstance(v, datetime):
                        raw_dict[k] = to_ch_datetime(v)

                try:
                    # msgspec is highly optimized. JobRecord.__post_init__ handles
                    # the timestamp sanitization automatically.
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
                get_current_timestamp(
                    timezone=self.exec_ctx.timezone, strip_tz=STRIP_TZ_FOR_DB
                )
            ),
            "JOB_STATUS": status,  # This will be handled by to_ch_datetime
        }
        new_record = self._create_updated_record(None, updates)

        with self._registry_lock:
            self._active_records[run_id] = new_record

        # Ensure the initial 'PROVISIONED' or 'PENDING' state hits the log stream
        with self._buffer_lock:
            self._update_buffer.append(new_record)
            if len(self._update_buffer) >= 50:
                self._flush_buffer_to_stream(threshold=50)

    def _flush_buffer_to_stream(self, threshold: int = 1, force: bool = False) -> None:
        """
        Writes in-memory updates to disk.
        - If force=True: Writes everything regardless of count.
        - If force=False: Only writes if buffer meets the threshold.
        """
        with self._buffer_lock:
            buffer_count = len(self._update_buffer)

            if not self._update_buffer or (
                not force and len(self._update_buffer) < threshold
            ):
                return

            LOG.debug(
                "Flushing state buffer to stream", count=buffer_count, forced=force
            )
            batch_data = b"".join(
                [msgspec.json.encode(r) + b"\n" for r in self._update_buffer]
            )
            try:
                with self.stream_path.open("ab") as f:
                    f.write(batch_data)
                self._update_buffer.clear()
            except OSError as e:
                LOG.error("Failed to flush buffer to disk", error=str(e))

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

    def flush(self) -> None:
        """Rotates JSONL to Parquet using optimized Lazy execution."""
        self._flush_buffer_to_stream(force=True)

        if not self.stream_path.exists() or self.stream_path.stat().st_size == 0:
            return

        batch_id = int(time.time())
        temp_jsonl = self.stage_dir / f"batch_{batch_id}.jsonl"
        target_parquet = temp_jsonl.with_suffix(".parquet")

        try:
            # Rotate file immediately to free up the stream
            self.stream_path.rename(temp_jsonl)

            # 1. Start Lazy Scan (Much faster for large JSONL)
            lf = pl.scan_ndjson(temp_jsonl)

            # 2. Dynamic Schema Alignment
            schema_df = self._get_target_schema()

            expressions = []
            for row in schema_df.to_dicts():
                col, raw_dtype = row["column_name"], row["data_type"]

                if col not in lf.columns:
                    continue

                target_ptype = TypeResolver.resolve_to_polars("clickhouse", raw_dtype)

                # Optimization: Build casting expressions once
                if "date" in raw_dtype.casefold():

                    expr = (
                        pl.col(col).str.to_date(strict=False)
                        if raw_dtype.casefold() == "date"
                        else pl.col(col).str.to_datetime(strict=False)
                    )
                    expressions.append(expr.cast(target_ptype))
                else:
                    expressions.append(pl.col(col).cast(target_ptype, strict=False))

            # 3. Collect and Write
            # select(expressions) applies all casts in a single parallel pass
            df: pl.DataFrame = lf.select(expressions).collect()

            if df.height > 0:
                with pl.Config(tbl_cols=-1):
                    print(df)
                df.write_parquet(target_parquet)
                temp_jsonl.unlink()  # Delete JSONL only after successful Parquet write

                # 4. Push to ClickHouse
                self._process_stage()

                self.last_flush = time.time()
                LOG.success("StateStore flush successful", rows=df.height)
            else:
                temp_jsonl.unlink()

        except Exception as e:
            LOG.error("StateStore flush failed", error=str(e))
            raise  # Reraise to ensure the Orchestrator knows the sync failed

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

        # Construct the typed update object. The JobUpdate class now handles
        # the to_ch_datetime sanitization internally.
        update_obj = JobUpdate(
            JOB_STATUS=status,
            CURRENT_STEP=manifest.current_stage.upper(),
            JOB_BITMASK=manifest.bitmask,
            RETRY_ATTEMPTS=manifest.retry_count,
            LAST_UPDATED_AT_TS_LC=get_current_timestamp(
                timezone=self.exec_ctx.timezone, strip_tz=STRIP_TZ_FOR_DB
            ),
            START_TIMESTAMP_LC=getattr(manifest.start, "start_timestamp", None),
            END_TIMESTAMP_LC=getattr(manifest.archive, "end_timestamp", None),
            SOURCE_ROW_COUNT=getattr(manifest.extract, "source_row_count", None),
            FINAL_ROW_COUNT=getattr(manifest.publish, "final_count", None),
            RUNTIME_OVERRIDES=context.custom_overrides or None,
            FINAL_MANIFEST=(
                msgspec.json.encode(manifest).decode("utf-8") if deep_sync else None
            ),
            REMARKS=metadata.get("remarks"),
        )

        # Delegate to the shared engine
        self.update_run(manifest.run_id, update_obj)

    def update_run(self, run_id: str, updates: JobUpdate | dict[str, Any]) -> None:
        """
        Applies partial updates to a tracked run and emits the new state.
        Perfect for heartbeats (.sync) or lightweight status changes (.retry).
        """
        # Normalize immutable updates into a dictionary for merging
        if isinstance(updates, JobUpdate):
            # Strip None values so they don't overwrite valid base data during merge
            update_dict = {
                k: v for k, v in msgspec.to_builtins(updates).items() if v is not None
            }
        else:
            update_dict = updates

        with self._registry_lock:
            current = self._active_records.get(run_id)
            if not current:
                LOG.warning(
                    "Update failed: Run ID not found in registry", run_id=run_id
                )
                return

            new_record = self._create_updated_record(current, update_dict)

            # Print the record as a formatted DataFrame row for debugging
            with pl.Config(tbl_cols=-1, fmt_str_lengths=50, tbl_width_chars=200):
                debug_df = pl.DataFrame([msgspec.to_builtins(new_record)])
                print(f"\n[STATE_UPDATE] {run_id}\n{debug_df}")
            if (
                msgspec.to_builtins(new_record).items()
                <= msgspec.to_builtins(current).items()
            ):
                return

            # --- NEW LOGIC: Only buffer if functional data changed ---
            # We compare the dicts but ignore the 'LAST_UPDATED' timestamp
            curr_dict = msgspec.to_builtins(current)
            new_dict = msgspec.to_builtins(new_record)

            # Remove timestamps from comparison to see if 'real' data changed
            curr_dict.pop("LAST_UPDATED_AT_TS_LC", None)
            new_dict.pop("LAST_UPDATED_AT_TS_LC", None)

            if curr_dict == new_dict:
                # No real change (just a heartbeat), don't clutter the log
                return

            # If we got here, something changed (Status, Bitmask, etc.)
            self._active_records[run_id] = new_record

        # Define the variable before the lock or ensure it's returned from it
        current_size = 0
        with self._buffer_lock:
            self._update_buffer.append(new_record)
            current_size = len(self._update_buffer)  # Assigned inside the lock

        # Now current_size is safely in scope for the check
        if current_size >= 50:
            self._flush_buffer_to_stream(threshold=50)
