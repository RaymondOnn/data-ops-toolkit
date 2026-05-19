import time
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any

import msgspec
import polars as pl
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.contexts.task import TaskContext
from apps.ingestion.src.core.models.stages.enums import EXEC_STAGES, StageName
from apps.ingestion.src.core.models.task import ExecutionStatus, TaskManifest
from apps.ingestion.src.core.orchestrator.enums import (
    JobRecord,
    JobUpdate,
    TaskRef,
    to_ch_datetime,
)
from apps.ingestion.src.services.database import DatabaseSink
from apps.ingestion.src.services.factory import ServiceFactory
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
    def __init__(self, db_config: dict, exec_ctx: ExecutionContext) -> None:
        self._db_config = db_config
        self.exec_ctx = exec_ctx
        self.workspace_dir = self.exec_ctx.state_path
        self._db: DatabaseSink | None = None

        # Decentralized: StateStore owns the state directory
        self.workspace_dir.mkdir(parents=True, exist_ok=True)

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
        self._buffer_lock = RLock()
        self._registry_lock = RLock()

    @property
    def db(self) -> DatabaseSink:
        """
        Lazy-loaded database connection client.
        Only runs when self.client is explicitly read for the first time.
        """
        if self._db is None:
            # Safely copy to avoid modifying the application settings out from under the app
            config = self._db_config.copy()
            service_type = config.pop("type")

            # Wake up the connection via your service factory
            self._db = ServiceFactory.get_service(service_type=service_type, **config)

        return self._db

    def close(self) -> None:
        """Flushes pending state and closes the database connection."""
        # Ensure any buffered or streamed state is pushed before closing the pipe
        try:
            self.flush()
        except Exception as e:
            LOG.error(f"Final state flush during close failed: {e}")

        if self._db is not None:
            LOG.debug("Closing database connection for StateStore.")
            self._db.close()
            self._db = None  # Reset to allow re-initialization if needed
        else:
            LOG.trace("No active database connection to close for StateStore.")

    def _get_target_schema(self) -> pl.DataFrame:
        """Retrieves and caches the schema for the log table."""
        if self._dest_schema_cache is None:
            # DESTINATION_TBL = "META.EXECUTION_LOG"
            # get_schema returns a DataFrame with column_name and data_type
            self._dest_schema_cache = self.db.client.get_schema(DESTINATION_TBL)
        return self._dest_schema_cache

    def _create_updated_record(
        self, base_record: JobRecord | None, updates: dict[str, Any]
    ) -> JobRecord | None:
        # Merge base record with updates
        merged = msgspec.to_builtins(base_record) if base_record else {}
        merged.update(updates)

        # Force timestamp update
        merged["LAST_UPDATED_AT_TS_LC"] = to_ch_datetime(
            get_current_timestamp(
                timezone=self.exec_ctx.timezone, strip_tz=STRIP_TZ_FOR_DB
            )
        )

        try:
            return msgspec.convert(merged, type=JobRecord)
        except msgspec.ValidationError as ve:
            LOG.error(
                "JobRecord validation failed during merge",
                error=str(ve),
                run_id=merged.get("RUN_ID"),
                updates=updates,
            )
            return None
        except Exception as e:
            LOG.error(
                "Unexpected error during JobRecord merge",
                error=str(e),
                run_id=merged.get("RUN_ID"),
                updates=updates,
            )
            return None

    @property
    def active_registry(self) -> dict[str, JobRecord]:
        with self._registry_lock:
            return self._active_records

    def resolve_task_path(self, identifier: str) -> Path | None:
        """Locates the physical directory for a task identifier or run_id."""
        from apps.ingestion.src.utils.common import find_path

        path = find_path(self.exec_ctx.workspace_dir, identifier)
        return path if path and path.exists() else None

    def create_record(self, task_ref: TaskRef) -> None:
        """Manually seeds the Active Registry for ad-hoc or dumb-mode runs."""
        run_id = task_ref.run_id
        if run_id in self._active_records:
            LOG.debug(
                "Run ID already exists in Active Registry; "
                "skipping manual creation to prevent overwrite.",
                run_id=run_id,
            )
            return

        # Use the internal helper to ensure consistent formatting and validation
        updates = {
            "JOB_ID": task_ref.job_id,
            "DATASET_ID": task_ref.dataset_id,
            "PARTITION_DATE": task_ref.partition_date,
            "RUN_ID": run_id,
            "IS_SCHEDULED": 0,
            "CURRENT_STAGE": task_ref.stage,
            "SCHEDULED_TIMESTAMP_LC": to_ch_datetime(
                get_current_timestamp(
                    timezone=self.exec_ctx.timezone, strip_tz=STRIP_TZ_FOR_DB
                )
            ),
            "JOB_STATUS": task_ref.status,
        }
        new_record = self._create_updated_record(None, updates)

        if not new_record:
            LOG.error("Failed to create initial JobRecord", run_id=run_id)
            return

        with self._registry_lock:
            self._active_records[run_id] = new_record

        # Print the record as a formatted DataFrame row for debugging
        with pl.Config(tbl_cols=-1, fmt_str_lengths=50, tbl_width_chars=200):
            debug_df = pl.DataFrame([msgspec.to_builtins(new_record)])
            print(f"\n[STATE_CREATE] {run_id}\n{debug_df}")

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

        try:
            # 1. Fast decode using msgspec
            with manifest_file.open("rb") as f:
                manifest = msgspec.json.decode(f.read(), type=TaskManifest)

            # 2. Resolve Context: Try disk config -> Registry fallback -> Placeholder
            ctx = None
            if config_file.exists():
                try:
                    with config_file.open("rb") as f:
                        # Use msgspec.json to match Orchestrator._trigger_job format
                        ctx = msgspec.json.decode(f.read(), type=TaskContext)
                except Exception as e:
                    LOG.warning(
                        "Failed to decode TaskContext during sync", error=str(e)
                    )

            if ctx is None:
                # If config is missing or corrupt, try to rehydrate from Active Registry
                record = self._active_records.get(manifest.run_id)
                if record:
                    ctx = TaskContext.create_placeholder(record)
                else:
                    LOG.error(
                        "Sync aborted: config.json missing and run not in registry",
                        run_id=manifest.run_id,
                    )
                    return

            # 3. Emit to the local stream immediately
            # We flag this as a 'SYNC' event in metadata if needed
            self._emit_state(
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

    @staticmethod
    def generate_progress_log(
        bitmask: int,
        status: str,
        planned_stages: Sequence[StageName] | None = None,
    ) -> str:
        """
        Converts a bitmask and stage list into a timeline string.
        Example: 7, [EXT, AUD, TRN, WRI] -> "EXT+ | AUD+ | TRN+ | WRI_"
        """
        planned_stages = planned_stages or EXEC_STAGES
        failure_detected = status.upper() == ExecutionStatus.FAILED.value
        failed_found = False
        results = []

        for stage in planned_stages:
            if bitmask & stage.bitmask:
                results.append(f"{stage.token}+")
            elif failure_detected and not failed_found:
                results.append(f"{stage.token}-")
                failed_found = True
            else:
                results.append(f"{stage.token}_")

        return " | ".join(results)

    def flush(self) -> None:
        """Rotates JSONL to Parquet using optimized Lazy execution."""
        batch_id = int(time.time())
        temp_jsonl = self.stage_dir / f"batch_{batch_id}.jsonl"
        target_parquet = temp_jsonl.with_suffix(".parquet")

        with self._buffer_lock:
            self._flush_buffer_to_stream(force=True)

            if not self.stream_path.exists() or self.stream_path.stat().st_size == 0:
                return

            try:
                # Rotate file inside the lock to prevent concurrent writes during rename
                self.stream_path.rename(temp_jsonl)
            except Exception as e:
                LOG.error(f"Failed to rotate stream file: {e}")
                return

        try:
            # 1. Dynamic Schema Alignment: Retrieve target structure from DB first

            # 1. Dynamic Schema Alignment: Retrieve target structure from DB first
            schema_df = self._get_target_schema()

            # CRITICAL: We provide an explicit schema to scan_ndjson to prevent Polars
            # from inferring a 'Null' type for columns that only contain nulls in the
            # first chunk of rows. This fixes "got non-null value for NULL-typed column".
            # We map all expected DB columns to pl.String for a stable foundation.
            scan_schema = {
                row["column_name"]: pl.String for row in schema_df.to_dicts()
            }

            # 2. Start Lazy Scan (Using explicit schema to bypass inference)
            lf = pl.scan_ndjson(temp_jsonl, schema=scan_schema)

            # Build Alignment Expressions
            lf_cols = {c.upper(): c for c in lf.columns}
            expressions = [
                self._get_alignment_expr(row, lf_cols) for row in schema_df.to_dicts()
            ]

            # 3. Collect and Write
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

    def _get_alignment_expr(
        self, row: dict[str, str], lf_cols: dict[str, str]
    ) -> pl.Expr:
        """Generates a Polars cast expression for a single DB column."""
        col, raw_dtype = row["column_name"], row["data_type"]
        db_col_upper = col.upper()
        target_ptype = TypeResolver.resolve_to_polars("clickhouse", raw_dtype)

        if db_col_upper not in lf_cols:
            return pl.lit(None).cast(target_ptype).alias(col)

        # Treat as string first to prevent pl.Null inference crashes
        expr = pl.col(lf_cols[db_col_upper]).cast(pl.String)

        if "date" in raw_dtype.casefold():
            if "datetime" in raw_dtype.casefold():
                # For DateTime/DateTime64, rely on ISO-8601 inference
                expr = expr.str.to_datetime(strict=False)
            else:
                # For Date/Date32, explicitly use the standard format to prevent ComputeError
                expr = expr.str.to_date(format="%Y-%m-%d", strict=False)

        return expr.cast(target_ptype, strict=False).alias(col)

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

    def _emit_state(
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

        status = str(metadata.get("status", manifest.status.value))
        progress_str = self.generate_progress_log(
            bitmask=manifest.bitmask, planned_stages=None, status=status
        )
        # Construct the typed update object. The JobUpdate class now handles
        # the to_ch_datetime sanitization internally.
        update_obj = JobUpdate(
            JOB_ID=manifest.job_id,
            DATASET_ID=manifest.dataset_id,
            PARTITION_DATE=context.partition_date,
            JOB_STATUS=status,
            CURRENT_STAGE=manifest.current_stage,
            JOB_BITMASK=progress_str,  # manifest.bitmask,
            RETRY_ATTEMPTS=manifest.retry_count,
            LAST_UPDATED_AT_TS_LC=get_current_timestamp(
                timezone=self.exec_ctx.timezone, strip_tz=STRIP_TZ_FOR_DB
            ).isoformat(sep=" "),
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
            if not new_record:
                LOG.warning("Update failed: Invalid data structure", run_id=run_id)
                return

            # --- NEW LOGIC: Only buffer if functional data changed ---
            # We compare the dicts but ignore the 'LAST_UPDATED' timestamp
            curr_dict = msgspec.to_builtins(current)
            new_dict = msgspec.to_builtins(new_record)

            c_ts = curr_dict.pop("LAST_UPDATED_AT_TS_LC", None)
            n_ts = new_dict.pop("LAST_UPDATED_AT_TS_LC", None)

            # Heartbeat Throttle: Even if no data changed, force a sync every 5 minutes (300s).
            # This ensures the 'Last Updated' column in the DB stays fresh for long tasks.
            time_since_last = 0
            if isinstance(c_ts, datetime) and isinstance(n_ts, datetime):
                time_since_last = (n_ts - c_ts).total_seconds()
            else:
                # If timestamps are missing or invalid, assume we need a sync
                time_since_last = 999

            if curr_dict == new_dict and time_since_last < 300:
                # No real change and heartbeat is still fresh
                LOG.trace(
                    "State update skipped: No functional data change", run_id=run_id
                )
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
