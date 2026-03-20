import hashlib
import time
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec
import polars as pl
import structlog
from src.core.models.job.manifest import ExtractPayload
from src.core.models.steps import JobStep
from src.core.strategies.extract import Reader, ReaderContext, ReaderFactory
from src.services.factory import ServiceFactory
from src.utils.exceptions import JobBlocked

from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitBreakerTripped

if TYPE_CHECKING:
    from src.core.models.job import Job


LOG = structlog.getLogger(__name__)


class FileInfo(msgspec.Struct):
    """
    Metadata for an individual physical file artifact.
    """

    path: str  # Absolute or relative path to the parquet file
    checksum: str  # MD5/SHA hash for forensic integrity
    row_count: int  # Number of rows in THIS specific file
    size_bytes: int  # Physical file size on disk


class ExtractStep(JobStep):
    manifest: ExtractPayload

    @property
    def name(self) -> str:
        return "extract"

    def execute(self, job: "Job") -> str:

        job_ctx = job.context

        try:
            # 1. Prepare Reader Context
            # This object is serialized and sent to Ray workers.
            # We include the schema_items from the manifest so workers
            # are 'Contract-Aware'

            # Resolve partition_date in filter_sql
            options = job_ctx.extract.source_params.copy()
            if "filter_sql" in options:
                # Suggestion: {partition_date} replaces {run_date}
                options["filter_sql"] = options["filter_sql"].replace(
                    "{partition_date}", job_ctx.run_date
                )

            ctx = ReaderContext(
                source_type=job_ctx.extract.source_type,
                source_identifier=job_ctx.extract.source_identifier,
                num_partitions=job_ctx.extract.num_partitions or 10,
                schema_items=job_ctx.extract.schema_items,
                run_id=job.run_id,
                run_date=job_ctx.run_date,
                job_id=job.id,
                options=options,
            )

            # 2. Extract & Guard (The Ray Orchestration)
            # Decision: DataReader.fetch uses the functional apply_schema_contract
            # inside the Ray workers to prevent double-handling.
            service = ServiceFactory.get_service(
                job_ctx.extract.source_identifier, **job_ctx.extract.source_config
            )
            reader: Reader = ReaderFactory.get_reader(ctx.source_type)
            LOG.info(f"Executing ingestion using strategy: {ctx.source_type}")

            # 3. We create a temporary physical folder in 'data'
            data_store = (
                job.exec_ctx.workspace_dir
                / "data"
                / self.name
                / f"{job.id}_{int(time.time())}"
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
                service=service,
                target_folder=data_store,
                context=ctx,
            )

            file_infos = []
            total_rows = 0
            all_schemas = []

            for f in extracted_files:
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

            # c. CALCULATE FINAL SCHEMA (The "Union" of all files)
            # This identifies all columns across all files, handling API drift.
            final_schema_dict = self._merge_schemas(all_schemas)

            # 6. Create Payload and Finalize
            # We map the strategy output to our ExtractPayload schema
            payload = ExtractPayload(
                artifact_folder=data_store,  # ?: Point to virtual or physical folder
                file_count=len(file_infos),
                files=file_infos,
                source_row_count=total_rows,
                # Grab schema from the last file processed
                schema_signature={k: str(v) for k, v in final_schema_dict.items()},
            )

            self.finalize(job, results=msgspec.to_builtins(payload))
            LOG.info("Reader completed", file_count=len(file_infos))

            # 4. State Transition
            return str(self._transit(job))
        except ClientCantConnect as ccc:
            LOG.error("Halt by client connection.", error=str(ccc))
            raise JobBlocked(str(ccc)) from ccc
        except CircuitBreakerTripped as cb:
            LOG.error("Halt by circuit breaker.", error=str(cb))
            raise
        except Exception as e:
            LOG.error(f"Extract Step failed: {e}")
            self.finalize(job, exception=e)
            raise

    def _calculate_checksum(self, path: Path) -> str:
        """
        Calculate the MD5 checksum of a file.
        Important to ensure data integrity and can be used for deduplication
        """
        hash_md5 = hashlib.md5()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _merge_schemas(self, schemas: list[dict[str, str]]) -> dict[str, str]:
        """
        Unions all schemas found in the source files to create a
        master schema for the Transform step.
        """
        merged = {}
        for schema in schemas:
            for col, dtype in schema.items():
                # In a real engine, you might add logic to handle
                # type conflicts (e.g., Float vs Int)
                merged[col] = str(dtype)
        return merged
