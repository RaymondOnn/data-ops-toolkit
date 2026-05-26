import hashlib
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec
import polars as pl
from apps.ingestion.src.core.models.task.manifest import ExtractPayload, FileInfo
from apps.ingestion.src.core.strategies.extract import (
    Reader,
    ReaderContext,
    ReaderFactory,
)
from apps.ingestion.src.services.factory import ServiceFactory
from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitBreakerTripped
from libs.utils.dates import get_current_timestamp
from loguru import logger

from .base import ExecutionStage
from .enums import StageName

if TYPE_CHECKING:
    from apps.ingestion.src.core.models.task import Task


LOG = logger


class ExtractStage(ExecutionStage):
    name = StageName.EXTRACT.label
    manifest: ExtractPayload

    def pre_flight(self, task: "Task") -> None:
        """Verify source connectivity from the execution node."""
        super().pre_flight(task)
        task_ctx = task.context
        self.service = ServiceFactory.get_source(
            task_ctx.extract.source_type, **task_ctx.extract.source_config
        )
        # Implementation would trigger a lightweight ping/exists check

    def execute(self, task: "Task") -> str:

        task_ctx = task.context
        start_ts = get_current_timestamp(strip_tz=True).isoformat(sep=" ")

        try:
            # 1. Prepare Reader Context
            # This object is serialized and sent to Ray workers.
            # We include the schema_items from the manifest so workers
            # are 'Contract-Aware'

            # Resolve partition_date in filter_sql
            options = task_ctx.extract.source_params.copy()

            # Unified lookup: Support both the new semantic name and the legacy SQL name
            condition = options.get("filter_condition") or options.get("filter_sql")

            if condition:
                # Resolve dynamic template variables
                condition = condition.replace(
                    "{partition_date}", task_ctx.partition_date
                )
                options["filter_condition"] = condition

            # --- RESOURCE-AWARE WORKER CALCULATION ---
            total_rows = self.service.get_total_count(
                str(task_ctx.extract.source_identifier), options.get("filter_condition")
            )

            # Calculate "Width Factor"
            # More columns = fewer rows per worker to stay under 2GB.
            num_columns = (
                len(task_ctx.extract.schema_items) or 20
            )  # Default to 20 if unknown

            rows_per_worker = max(50_000, 20_000_000 // num_columns)

            target_num_workers = task_ctx.extract.num_workers
            if not target_num_workers:
                target_num_workers = max(1, min(100, total_rows // rows_per_worker))
                LOG.info(
                    f"Ingestion Scale: {total_rows} rows, {num_columns} cols. "
                    f"Targeting {rows_per_worker} rows/worker -> {target_num_workers} workers."
                )

            ctx = ReaderContext(
                source_type=task_ctx.extract.source_type,
                source_identifier=task_ctx.extract.source_identifier,
                num_workers=target_num_workers,
                schema_items=task_ctx.extract.schema_items,
                run_id=task.run_id,
                partition_date=task_ctx.partition_date,
                job_id=task.job_id,
                workspace_dir=str(task.exec_ctx.workspace_dir),
                options=options,
            )

            # 2. Extract & Guard (The Ray Orchestration)
            # Decision: DataReader.fetch uses the functional apply_schema_contract
            # inside the Ray workers to prevent double-handling.
            reader: Reader = ReaderFactory.get_reader(ctx.source_type)
            LOG.info(
                "Executing ingestion strategy",
                stage=self.name,
                strategy=ctx.source_type,
                source=ctx.source_identifier,
            )

            # 3. Get the deterministic physical folder from the workspace
            data_store = task.workspace.clear_stage_data(self.name)

            # 4. Execute the Ingestion
            # This calls reader.fetch() which:
            #   a. Asks service for work units (SQL queries/File paths)
            #   b. Distributes tasks to Ray workers
            #   c. Workers call service.fetch_stream() [Protected by Circuit Breaker]
            #   d. Workers call _apply_schema_contract
            #   e. Streams results to Parquet files (keeping RAM < 2GB)
            extracted_files = reader.fetch(
                service=self.service,
                target_folder=data_store,
                context=ctx,
            )

            if not extracted_files:
                LOG.warning(
                    "Ingestion returned no data",
                    stage=self.name,
                    source=ctx.source_identifier,
                    job_id=task.job_id,
                    run_id=task.run_id,
                )

            file_infos = []
            total_rows = 0
            all_schemas = []

            for i, f in enumerate(extracted_files):
                path = f["path"]

                # A. Calculate Checksum (MD5 or SHA256)
                checksum = self._calculate_checksum(f["path"])

                # Use Polars to get the schema of this specific file
                # This is fast as it only reads the parquet metadata
                file_schema = pl.read_parquet_schema(path)
                all_schemas.append(file_schema)

                # B. Build FileInfo
                file_infos.append(
                    FileInfo(
                        path=str(f["path"]),
                        checksum=checksum,
                        row_count=f["rows"],
                        size_bytes=f["path"].stat().st_size,
                    )
                )
                total_rows += f["rows"]

                # C. Checkpoint: Update manifest every 1 files (Optimization)
                # This updates the 'last_modified' timestamp on disk,
                # providing a physical heartbeat for the engine.
                if i % 1 == 0:
                    task.update_manifest(
                        {
                            "extract": {
                                "source_row_count": total_rows,
                                "file_count": len(file_infos),
                            }
                        }
                    )
                    # No request_status_sync needed here anymore

            # c. CALCULATE FINAL SCHEMA (The "Union" of all files)
            # This identifies all columns across all files, handling API drift.
            final_schema_dict = self._merge_schemas(all_schemas)

            # 5. Resolve Final Audit Identity
            # We look at the actual files discovered to decide the _source value
            source_files = getattr(reader, "source_files", [])
            audit_identity = self._resolve_audit_identity(ctx, source_files)

            # 6. Create Payload and Finalize
            # We map the strategy output to our ExtractPayload schema
            payload = ExtractPayload(
                artifact_folder=str(data_store),
                source_identifier=audit_identity,
                source_files=source_files,
                file_count=len(file_infos),
                files=file_infos,
                source_row_count=total_rows,
                # Grab schema from the last file processed
                schema_signature={k: str(v) for k, v in final_schema_dict.items()},
                start_timestamp=start_ts,
            )

            self.finalize(
                task, data_folder=data_store, results=msgspec.to_builtins(payload)
            )
            LOG.info(
                "Reader completed",
                stage=self.name,
                file_count=len(file_infos),
                total_rows=total_rows,
            )

            # 4. State Transition
            return str(self._transit(task))
        except (ClientCantConnect, CircuitBreakerTripped) as e:
            LOG.warning(f"Ingestion halted: {e}", stage=self.name)
            self.finalize(task, exception=e)
            raise
        except Exception as e:
            LOG.exception("Extract Step failed", stage=self.name)
            self.finalize(task, exception=e)
            raise

    def _calculate_checksum(self, path: Path) -> str:
        """
        Calculate the MD5 checksum of a file.
        Important to ensure data integrity and can be used for deduplication
        """
        LOG.debug("Calculating checksum", stage=self.name, file=str(path))
        hash_md5 = hashlib.md5()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _resolve_audit_identity(
        self, context: ReaderContext, source_files: list[str]
    ) -> str:
        """
        Determines the string identifier used for the '_source' audit column.

        Rules:
        1. If exactly one source file was found, use its filename.
        2. If multiple files (batch) or a database table, use the name
           of the source identifier (the folder name or table name).
        """
        if len(source_files) == 1:
            return Path(source_files[0]).name

        # For batches or DBs, we take the terminal portion of the identifier
        # rstrip handles trailing slashes for folders
        base_ident = context.source_identifier or ""
        return Path(base_ident.rstrip("/")).name or base_ident

    def _merge_schemas(self, schemas: list[dict[str, str]]) -> dict[str, str]:
        """
        Unions all schemas found in the source files to create a
        master schema for the Transform stage.
        """
        LOG.debug(
            "Merging schemas from extracted files",
            stage=self.name,
            file_count=len(schemas),
        )
        merged = {}
        for schema in schemas:
            for col, dtype in schema.items():
                dtype_str = str(dtype)
                if col not in merged:
                    merged[col] = dtype_str
                    continue

                # When dtype of the same column differs across files
                # we favor string or UTF-8 or float types to avoid data loss.
                current = merged[col].lower()
                new = dtype_str.lower()

                if current != new:
                    if "float" in new or "double" in new:
                        merged[col] = dtype_str
                    elif "string" in new or "utf8" in new:
                        merged[col] = dtype_str  # String wins over everything

        return merged
