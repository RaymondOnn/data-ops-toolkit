import hashlib
import time
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
        start_ts = get_current_timestamp(strip_tz=True)
        try:
            # 1. Prepare Reader Context
            # This object is serialized and sent to Ray workers.
            # We include the schema_items from the manifest so workers
            # are 'Contract-Aware'

            # Resolve partition_date in filter_sql
            options = task_ctx.extract.source_params.copy()
            if "filter_sql" in options:
                # Suggestion: {partition_date} replaces {partition_date}
                options["filter_sql"] = options["filter_sql"].replace(
                    "{partition_date}", task_ctx.partition_date
                )

            ctx = ReaderContext(
                source_type=task_ctx.extract.source_type,
                source_identifier=task_ctx.extract.source_identifier,
                num_workers=task_ctx.extract.num_workers or 10,
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

            # 3. We create a temporary physical folder in 'data'
            data_store = (
                task.exec_ctx.workspace_dir
                / "data"
                / self.name
                / f"{task.job_id}_{int(time.time())}"
            )
            data_store.mkdir(parents=True, exist_ok=True)

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

            # 6. Create Payload and Finalize
            # We map the strategy output to our ExtractPayload schema
            payload = ExtractPayload(
                artifact_folder=str(
                    data_store
                ),  # ?: Point to virtual or physical folder
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

    def _merge_schemas(self, schemas: set[dict[str, str]]) -> dict[str, str]:
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
                # In a real engine, you might add logic to handle
                # type conflicts (e.g., Float vs Int)
                merged[col] = str(dtype)
        return merged
