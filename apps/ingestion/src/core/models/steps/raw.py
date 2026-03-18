import time
from pathlib import Path

import msgspec
import polars as pl
import structlog
from src.core.models.job import Job
from src.core.models.job.manifest import RawPayload
from src.core.models.steps import JobBitmask, JobStep
from src.services.factory import ServiceFactory
from src.utils.constants import JOB_STEPS_BASE_DIR
from src.utils.exceptions import JobBlocked

from libs.clients.base import ClientCantConnect
from libs.resilience.circuit_breaker import CircuitBreakerTripped

LOG = structlog.getLogger(__name__)


class FileInfo(msgspec.Struct):  # type: ignore
    """
    Metadata for an individual physical file artifact.
    """

    path: str  # Absolute or relative path to the parquet file
    checksum: str  # MD5/SHA hash for forensic integrity
    row_count: int  # Number of rows in THIS specific file
    size_bytes: int  # Physical file size on disk


class RawStep(JobStep):  # type: ignore
    manifest: RawPayload

    @property
    def bitmask(self) -> str:
        return str(JobBitmask.RAW)

    @property
    def name(self) -> str:
        return "raw"

    def execute(self, job: "Job") -> str:
        from src.core.strategies.ingest.factory import ReaderFactory
        from src.core.strategies.ingest.ingest import Reader, ReaderContext

        job_ctx = job.context

        try:
            # 1. Prepare Reader Context
            # This object is serialized and sent to Ray workers.
            # We include the schema_items from the manifest so workers are 'Contract-Aware'
            ctx = ReaderContext(
                source_type=job_ctx.source_type,
                target_table=job_ctx.target_table,
                num_partitions=job_ctx.num_partitions or 10,
                schema_items=job_ctx.schema_items,
            )

            # 2. Extract & Guard (The Ray Orchestration)
            # Decision: DataReader.fetch uses the functional apply_schema_contract
            # inside the Ray workers to prevent double-handling.
            service = ServiceFactory.get_service(
                job_ctx.source_type, **job_ctx.source_config
            )
            reader: Reader = ReaderFactory.get_reader(ctx.source_type)
            LOG.info(f"Executing raw ingestion using strategy: {ctx.source_type}")

            # 3. We create a temporary physical folder in 'data'
            data_store = (
                JOB_STEPS_BASE_DIR / "data" / self.name / f"{job.id}_{int(time.time())}"
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
            # We map the strategy output to our RawPayload schema
            payload = RawPayload(
                step_outcome="COMPLETED",
                artifact_folder=data_store,  # ?: Point to virtual or physical folder
                file_count=len(file_infos),
                files=file_infos,
                raw_row_count=total_rows,
                # Grab schema from the last file processed
                schema_signature={k: str(v) for k, v in final_schema_dict.items()},
            )

            self.finalize(job, results=msgspec.to_builtins(payload))

            # 4. State Transition
            return str(self._transit(job))
        except ClientCantConnect as ccc:
            LOG.error("Halt by client connection.", error=str(ccc))
            raise JobBlocked(str(ccc)) from ccc
        except CircuitBreakerTripped as cb:
            LOG.error("Halt by circuit breaker.", error=str(cb))
            raise
        except Exception as e:
            LOG.error(f"Raw ingestion failed: {e}")
            self.finalize(job, exception=e)
            raise

    def _calculate_checksum(self, path: Path) -> str:
        """
        Calculate the MD5 checksum of a file.
        Important to ensure data integrity and can be used for deduplication
        """
        import hashlib

        hash_md5 = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _merge_schemas(self, schemas: list[dict[str, str]]) -> dict[str, str]:
        """
        Unions all schemas found in the raw files to create a
        master schema for the Transform step.
        """
        merged = {}
        for schema in schemas:
            for col, dtype in schema.items():
                # In a real engine, you might add logic to handle
                # type conflicts (e.g., Float vs Int)
                merged[col] = str(dtype)
        return merged
