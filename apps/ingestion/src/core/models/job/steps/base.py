import os
import shutil
import socket
import time
import traceback
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from apps.ingestion.src.core.models.job import manifest
import msgspec # type: ignore
import polars as pl # type: ignore
import structlog # type: ignore
from src.core.load.load import Loader, WriteContext
from src.core.models.job.manifest import (
    AuditPayload,
    BasePayload,
    CompletePayload,
    ErrorPayload,
    JobManifest,
    PublishPayload,
    RawPayload,
    TransformPayload,
    WritePayload,
)
from src.services.factory import ServiceFactory
from src.utils.constants import JOB_STEPS_BASE_DIR
from src.utils.exceptions import JobFailed, JobBlocked, JobDeferred
from libs.file.formats.parquet import ParquetHandler
from libs.clients.base import ClientCantConnect



from libs.resilence.circuit_breaker import CircuitBreakerTripped

if TYPE_CHECKING:
    from src.core.models.job.base import Job

LOG = structlog.getLogger(__name__)
_JOB_ORDER = [
    "start",
    "raw",
    "transform",
    "write",
    "audit",
    "publish",
    "complete",
]


class JobBitmask:
    """
    Class representing the bitmask for job steps.
    """

    START: str = "0000001"
    RAW: str = "0000010"
    TRANSFORM: str = "0000100"
    WRITE: str = "0001000"
    AUDIT: str = "0010000"
    PUBLISH: str = "0100000"
    COMPLETE: str = "1000000"


class JobStep(ABC):
    """Base class for JobStage classes."""

    @property
    def bitmask(self) -> str:
        raise NotImplementedError("Subclasses must implement this property")

    @property
    def name(self) -> str:
        raise NotImplementedError("Subclasses must implement this property")

    def get_step(self, offset: int) -> str:
        idx = _JOB_ORDER.index(self.name)
        if 0 <= idx + offset < len(_JOB_ORDER):
            return _JOB_ORDER[idx + offset]
        else:
            raise ValueError(f"Invalid offset: {offset}")
    
    @abstractmethod
    def execute(self, job: "Job") -> str:
        """Execute the current JobStage with the given engine and dataframe.

        :param engine: The IngestionEngine instance.
        :type engine: IngestionEngine
        :param df: The dataframe to process in this stage.
        :type df: Optional[Any]
        """
        raise NotImplementedError

    def _transit(self, job: "Job") -> str:
        """Transit the Job instance to the next stage."""
        next_step = self.get_step(offset=1)
        job.set_step(JobStep.get_step_class_by_name(next_step))
        return next_step
        return "FINISH"

    def finalize(
        self,
        job: "Job",
        data_folder: Path | None = None,
        results: dict[str, Any] | None = None,
        exception: Exception | None = None,
    ) -> None:
        """
        DECISION: Deterministic Paths & Symlinking.
        We avoid searching for 'latest' folders by using a static symlink 
        at active/{job_id}/{step_name}.
        """
        results = results or {}

        # 1. READ & BOOTSTRAP
        # Check if file exists and has content
        if not job.folder:
            raise ValueError("Job folder is not set")

        manifest_path = job.folder / "manifest.json"
        if manifest_path.exists() and manifest_path.stat().st_size > 0:
            with open(manifest_path, "rb") as f:
                data: dict[str, Any] = msgspec.json.decode(f.read())
        else:
            # File is empty or doesn't exist: Start Phase
            data = {
                "job_id": job.id,
                "run_id": job.run_id,
                "dataset_name": job.context.dataset_name,
                "status": "RUNNING",
                "current_step": "init",
            }

        # 2. MUTATE (same as before)
        if exception:
            error_payload = msgspec.to_builtins(
                ErrorPayload(
                    step=self.name,
                    error_type=type(exception).__name__,
                    message=str(exception),
                    stack_trace=traceback.format_exc(),
                    worker_id=job.worker_id,
                )
            )
            data["status"] = "FAILED"
            data["error"] = error_payload
            
            if isinstance(exception, JobFailed)
        else:
            if data["current_step"] == "complete":
                data["status"] = "COMPLETED"
            data["current_step"] = self.name
            if results:
                data[self.name] = results

        # 3. CREATE SYMLINK
        if data_folder:
            active_link = job.folder / self.name
            if active_link.exists() or active_link.is_symlink():
                active_link.unlink()
            
            # Create the pointer to the immutable physical data
            relative_target = Path("..") / ".." / "data" / self.name / data_folder.name
            active_link.symlink_to(relative_target)
        
        # 4. ATOMIC SWAP
        manifest = msgspec.convert(data, JobManifest)
        job.update_status(manifest)
        

    @classmethod
    def get_step_class_by_name(cls, name: str) -> "JobStep":
        """
        Given a step name, returns the corresponding JobStep class.

        Iterates through all subclasses of JobStep and checks if the name attribute matches the given name.
        If no match is found, raises a ValueError.
        """

        for cls in cls.__subclasses__():
            # If you have nested subclasses, you may want a recursive walk here.
            if (
                getattr(cls, "name", None) == name
                or getattr(cls(), "name", None) == name
            ):
                return cls
        raise ValueError(f"Unknown step name: {name}")
    
    def get_manifest(self, job: "Job") -> JobManifest:
        """Helper to read the current state of the world."""
        if job.manifest_path and not job.manifest_path.exists():
            raise FileNotFoundError(f"Manifest missing at {job.manifest_path}")

        with open(job.manifest_path, "rb") as f:
            return msgspec.json.decode(f.read(), type=JobManifest)
        
class StartStep(JobStep):
    manifest: BasePayload

    @property
    def bitmask(self) -> str:
        return JobBitmask.START

    @property
    def name(self) -> str:
        return "start"

    def execute(self, job: "Job") -> str:
        


        # persist job-start metadata using engine helper
        try:
            # 3. Gather System Metadata
            start_timestamp = datetime.now().isoformat()
            commit_hash = self._get_commit_hash()  # Use the helper above
            worker_id = f"{socket.gethostname()}-{os.getpid()}"

            # 4. Create Payload
            payload = BasePayload(
                step_outcome="COMPLETED",
                commit_hash=commit_hash,
                source_params={},
                worker_id=job.worker_id,
                start_timestamp=start_timestamp,
            )
            ctx = msgspec.structs.asdict(payload)

            # 5. Finalize (using the generic helper we discussed)
            # Note: Pass the Struct directly if finalize() handles to_builtins
            self.finalize(job, results=ctx)
            LOG.info("Job initialized", job_id=job.id, run_id=job.run_id)
            return self._transit(job)
        except Exception as e:
            # Ensure we capture the traceback in the manifest
            self.finalize(job, exception=e)
            raise

    def _get_commit_hash(self) -> str:
        import subprocess

        try:
            # Returns the short hash (e.g., a1b2c3d)
            return (
                subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
                .decode("ascii")
                .strip()
            )
        except Exception:
            return "unknown"


class FileInfo(msgspec.Struct): # type: ignore
    """
    Metadata for an individual physical file artifact.
    """

    path: str  # Absolute or relative path to the parquet file
    checksum: str  # MD5/SHA hash for forensic integrity
    row_count: int  # Number of rows in THIS specific file
    size_bytes: int  # Physical file size on disk


class RawStep(JobStep):
    manifest: RawPayload

    @property
    def bitmask(self) -> str:
        return JobBitmask.RAW

    @property
    def name(self) -> str:
        return "raw"

    def execute(self, job: "Job") -> str:
        from src.core.ingest.factory import ReaderFactory
        from src.core.ingest.ingest import Reader, ReaderContext

        if not job.folder:
            raise ValueError("Job folder is not set.")

        try:
            # 1. Prepare Reader Context
            # This object is serialized and sent to Ray workers.
            # We include the schema_items from the manifest so workers are 'Contract-Aware'
            ctx = ReaderContext(
                source_type=job.context.source_type,
                target_table=job.context.target_table,
                num_partitions=job.context.num_partitions or 10,
                schema_items=job.context.schema_items,
            )
            
            # 2. Extract & Guard (The Ray Orchestration)
            # Decision: DataReader.fetch uses the functional apply_schema_contract 
            # inside the Ray workers to prevent double-handling.
            service = ServiceFactory.get_service(
                job.context.source_type,
                **job.context.source_params
            )
            reader: Reader = ReaderFactory.get_reader(ctx.source_type)
            LOG.info(f"Executing raw ingestion using strategy: {ctx.source_type}")

            # 3. We create a temporary physical folder in 'data'
            data_store = JOB_STEPS_BASE_DIR / "data" / self.name / f"{job.id}_{int(time.time())}"
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
                artifact_folder=str(data_store), #?: Point to virtual or physical folder
                file_count=len(file_infos),
                files=file_infos,
                raw_row_count=total_rows,
                # Grab schema from the last file processed
                schema_signature={k: str(v) for k, v in final_schema_dict.items()},
            )

            self.finalize(job, results=msgspec.to_builtins(payload))

            # 4. State Transition
            return self._transit(job)
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


class TransformStep(JobStep):
    manifest: TransformPayload

    @property
    def bitmask(self) -> str:
        return JobBitmask.TRANSFORM

    @property
    def name(self) -> str:
        return "transform"

    def execute(self, job: "Job") -> str:
        """
        Decision: Use LazyFrame Streaming for 50M rows.
        By reading from the 'active/raw' symlink, we ensure we are 
        always processing the latest sanitized data without needing 
        to know the specific physical timestamped folder.
        """
        from src.core.transform.transform import TransformFactory
        start_time = time.perf_counter()

        try:
            with ParquetHandler() as handler:
                # 1. Initialize the LazyFrame (Logical Plan)
                # Decision: Use the 'active' symlink path. 
                # Polars scans the metadata of all part_*.parquet files instantly.
                raw_path = str(JOB_STEPS_BASE_DIR / "active" / job.id / "raw" / "part_*.parquet")
                lf = handler.to_df(raw_path)
            
                # 2. Apply Business Logic (Transformers)
                # These add to the 'Plan' but do not execute yet.
                # Decision: Use the factory to apply bitmasking and custom logic.
                transformer = TransformFactory.get_transformer(job.context)
                tr_lf = transformer.apply(lf)


                # 3. Stream to Physical Storage
                # Decision: Use sink_parquet via our handler's execution-aware logic.
                # This triggers the Polars Rust engine to stream chunks through the plan.
                data_store = JOB_STEPS_BASE_DIR / "data" / self.name / f"{job.id}_{int(time.time())}"
                data_store.mkdir(parents=True, exist_ok=True)
                
                # 4. Decision: Use a partitioned sink. 
                # This creates part-0.parquet, part-1.parquet, etc., in the data_store folder.
                # This is much safer for 2GB RAM as it flushes buffers more frequently.
                handler.from_df(tr_lf, str(data_store))
                
            # 5. DECISION: Get accurate stats after the stream is closed
            # scan_parquet + select(len) on the OUTPUT directory reads only the 
            # file footers. This is near-instant even for 50M rows.
            stats = (
                pl.scan_parquet(str(data_store / "*.parquet"))
                .select(
                    count=pl.len(),
                    schema=pl.map_batches(lambda _: str(getattr(tr_lf, "schema"))) 
                )
                .collect() # This is safe because it's only 1 row of metadata
            )
            
            # 6. Build the RefinedMetadata
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            
            payload = TransformPayload(
                step_outcome="COMPLETED",
                logic_version=getattr(transformer, "version", "1.0.0"),
                artifact_folder=str(data_store),
                output_record_count=stats["count"][0],
                schema_validation_passed=True,  # Strategy could add validation logic
                refined_schema=stats["schema"][0],
                processing_duration_secs=duration_ms,
            )
            
            # 4. Finalize & Flip the Link
            # Decision: Create active/{job_id}/transform -> ../../data/transform/{dir}
            # This makes the transformed data available for the WriteStep.
            self.finalize(job=job, data_folder=data_store, results=payload)
            return self._transit(job)

        except Exception as e:
            self.finalize(job=job,  exception=e)
            raise


class WriteStep(JobStep):
    manifest: WritePayload

    @property
    def bitmask(self) -> str:
        return JobBitmask.WRITE

    @property
    def name(self) -> str:
        return "write"

    def execute(self, job: "Job") -> str:
        start_time = time.perf_counter()

        try:
            # 1. Resolve logical input (The partitioned parquet files)
            source_dir = JOB_STEPS_BASE_DIR / "active" / job.id / "transform"
        
            # 1. Get the Service (Securely initialized on Ray worker via ServiceFactory)
            service = ServiceFactory.get_service(
                job.context.sink_type, **job.context.sink_config
            )

            # 2. Get the behavioral Strategy
            loader = Loader()

            # 2. PHASE 1: LOAD TO STAGING
            staging_results = loader.load(
                service=service,
                source_dir=source_dir,
                target_table=job.context.target_destination,
            )

            # 3. Finalize Manifest
            payload = WritePayload(
                step_outcome="COMPLETED",
                target_identifier=job.context.target_destination,
                sink_type=job.context.destination_type,
                staging_artifact=(
                    staging_results.staging_path or staging_results.staging_table,
                ),
                rows_inserted=staging_results.rows,
                partition_col=job.context.partition_col,
                partition_value=job.context.partition_value,
                db_connection_id=service.connection_id,
                duration_secs=int((time.perf_counter() - start_time) * 1000),
            )

            self.finalize(job, results=payload)
            return self._transit(job)

        except Exception as exc:
            payload = ErrorPayload(
                step_name=self.name,
                error_type=type(exc).__name__,
                message=str(exc),
                stack_trace=traceback.format_exc(),
                is_transient=isinstance(
                    exc, (requests.RequestException, ConnectionError)
                ),
            )
            self.finalize(job, exception=payload)
            raise


class AuditStep(JobStep):
    manifest: AuditPayload

    @property
    def bitmask(self) -> str:
        return JobBitmask.AUDIT

    @property
    def name(self) -> str:
        return "audit"

    def execute(self, job: "Job") -> str:
        start_time = time.perf_counter()

        try:
            manifest: JobManifest = self.get_manifest(job)
            write_meta: WritePayload = (
                manifest.write
            )  # Access staging info from WriteStep

            # 1. Internal Heuristic Checks (The 'Stand-in' Logic)
            # While the external app is missing, we check basic things:
            # - Did we lose more than 10% of data?
            # - Are there nulls in the Primary Key?
            internal_results = self._run_internal_checks(job, write_meta)

            # 2. External App Mock Hook
            # This is where you will eventually call your validation API
            external_status = "NOT_AVAILABLE"

            # # 1. Construct the Shell Command
            # # We pass the staging artifact (table/path) as an argument to the app
            # cmd = [
            #     "validation-app",
            #     "--source", write_meta.staging_artifact,
            #     "--job-id", job.id,
            #     "--run-id", job.run_id
            # ]

            # # 2. Run the Command
            # # capture_output=True allows us to save the logs into our manifest
            # process = subprocess.run(
            #     cmd,
            #     capture_output=True,
            #     text=True,
            #     check=False  # We handle the error manually to finalize the manifest
            # )

            # 3. Build Payload
            duration_ms = int((time.perf_counter() - start_time) * 1000)

            payload = AuditPayload(
                step_outcome="COMPLETED",
                validation_passed=internal_results["passed"],
                total_checks_run=len(internal_results["checks"]),
                failed_checks=internal_results["failures"],
                external_app_status=external_status,
                audit_duration_ms=duration_ms,
            )

            self.finalize(job, results=payload)

            # If validation fails, we stop the pipeline here!
            if not payload.validation_passed:
                raise ValueError(
                    f"Audit failed for Job {job.id}. See manifest for details."
                )

            return self._transit(job)

        except Exception as e:
            self.finalize(job, exception=e)
            raise

    def _run_internal_checks(
        self, job: "Job", write_meta: WritePayload
    ) -> dict[str, Any]:
        """Simple baseline checks while the real app is under construction."""
        # Example: Check if rows_affected is 0
        checks = []
        failures = []

        # Check 1: Row count > 0
        checks.append("row_count_not_zero")
        if write_meta.rows_affected == 0:
            failures.append(
                {
                    "check": "row_count_not_zero",
                    "message": "No rows were loaded to staging.",
                }
            )

        return {"passed": len(failures) == 0, "checks": checks, "failures": failures}


class PublishStep(JobStep):
    """
    Decision: The PublishStep makes the data 'Public'.
    We use the context to identify the target 'Prod' table vs 'Staging' table.
    """
    manifest: PublishPayload

    @property
    def bitmask(self) -> str:
        return JobBitmask.PUBLISH

    @property
    def name(self) -> str:
        return "publish"

    def execute(self, job: "Job") -> str:
        start_time = time.perf_counter()

        try:
            manifest = self.get_manifest(job)
            write_meta = manifest.write
            if not write_meta:
                raise ValueError("Write metadata not found in manifest.")

            # 1. Get the Service (Securely initialized on Ray worker via ServiceFactory)
            service = ServiceFactory.get_service(
                job.context.sink_type, **job.context.sink_config
            )

            # 2. Get the behavioral Strategy
            loader = Loader()

            # 3. Create Context
            context = WriteContext(
                target=job.context.target_table,
                partition_col=job.context.partition_col,
                partition_value=job.context.partition_value,
            )

            # 2. FINISH THE JOB
            # Move from staging to production
            loader.promote(
                service=service,
                staging_info=write_meta.staging_artifact,
                write_ctx=context,
            )
            LOG.info("Job Published", job_id=job.id, table=job.context.target_destination)
            
            # 3. PAYLOAD: The 'Success Receipt'
            duration_ms = round(time.time() - start_time, 2)
            payload = PublishPayload(
                step_outcome="COMPLETED",
                final_destination=job.context.target_identifier,
                promotion_duration_secs=duration_ms,
                completed_at=datetime.now().isoformat(),
            )

            self.finalize(job, results=payload)
            return self._transit(job)  # This likely triggers 'FINISH'

        except Exception as e:
            self.finalize(job, exception=e)
            raise


class CompleteStep(JobStep):
    manifest: CompletePayload

    @property
    def bitmask(self) -> str:
        return JobBitmask.COMPLETE

    @property
    def name(self) -> str:
        return "complete"

    def execute(self, job: "Job") -> str:
        """
        Decision: The 'Zero-Footprint' Protocol.
        We preserve the audit trail and the output data in long-term storage
        while reclaiming high-speed local disk space.
        """
        from src.services.factory import ServiceFactory
        from src.services.file import StorageService

        
        self.manifest = self.get_manifest(job)
        
        # 1. Initialize Storage Service for Archival
        # We retrieve the 'archive' service defined in the job configuration
        object_store: StorageService = ServiceFactory.get_service(
            type=job.context.archive_type,  # e.g., "s3" or "local"
            **job.context.archive_config,
        )

        try:
            # 2. OPTIONAL ARCHIVAL
            # Subject to privacy requirements defined in job_config
            final_archive_path = None
            if job.context.enable_archival:
                # 1. Archive Parquet Files
                # We move data from the high-speed 'data/' vault to the 'archive/' vault.
                # This includes both the Raw (Sanitized) and Transform results.
                self._archive_parquet_data(object_store, job)
                
                # Service-level move (2GB RAM safe)
                final_archive_path = object_store.archive_data(
                    source_dir=raw_folder, archive_path=dest_path
                )

            # 3. CLEANUP VERIFICATION
            # Force removal of all intermediate data (Raw & Transform folders)
            local_run_root = job.folder.parent
            for folder in ["raw", "transform"]:
                target = local_run_root / folder
                if target.exists():
                    shutil.rmtree(target)

            # 4. Calculate Timestamps and Duration
            start_ts = datetime.fromisoformat(manifest.start.ingestion_started_at)
            end_ts = datetime.now()
            duration_secs = (end_ts - start_ts).total_seconds()

            # 5. FINALIZE CANONICAL PAYLOAD
            payload = CompletePayload(
                execution_outcome="SUCCESS",
                end_timestamp=end_ts.isoformat(),
                total_duration_secs=round(duration_secs, 2),
                cleanup_verified=True,
                archival_path=final_archive_path,
                retention_expiry=self._calculate_expiry(job, end_ts),
            )

            self.finalize(job, results=payload)
            
            # 2. Store Manifest in Database (Current Execution Table)
            # Decision: By moving manifest data to SQL, we allow the BI team to 
            # monitor job performance without needing file system access.
            self._record_execution_to_db(job)
            
            # 4. Final Finalize (Post-Purge)
            # We don't use a symlink here; we just record SUCCESS in the DB/State Store
            LOG.info("Job lifecycle complete. Workspace purged.", job_id=job.id)

            # This marks the final state of the manifest
            return "FINISH"

        except Exception as e:
            self.finalize(job, exception=e)
            raise

    def _archive_parquet_data(self, object_store: StorageService, job: "Job") -> None:
        """
        Decision: Move files to the Archive location defined in the Context.
        Standardizing on: archive/{job_id}/{run_id}/{step}/
        """
        archive_root = f"{job.context.archive_base_path}/{job.id}/{job.run_id}"
        
        # We loop through the steps we want to keep
        for step in ["raw", "transform"]:
            # Follow the active symlink to find the physical data
            src_folder = job.folder.resolve() / step
            if src_folder.exists():
                dest_folder = f"{archive_root}/{step}"
                # target_archive.mkdir(parents=True, exist_ok=True)
                final_archive_path = object_store.archive_data(
                    source_dir=src_folder, archive_path=dest_folder
                )
                
    def _calculate_expiry(self, job: "Job", end_timestamp: datetime) -> str:
        # e.g., standard 7-year retention or 30-day GDPR limit
        """
        Calculates the retention expiry date for a job.

        Uses the retention_days attribute from the JobContext if present,
        otherwise falls back to a 7-year default.

        Returns an ISO-formatted string representing the retention expiry date.
        """
        retention_days = getattr(
            job.context, "retention_days", 2555
        )  # 7 years default
        return (end_timestamp + timedelta(days=retention_days)).date().isoformat()
    
    def _record_execution_to_db(self, job: "Job"):
        """
        Decision: Upsert final stats into the 'job_execution_history' table.
        This provides a high-level audit trail for 50M row jobs.
        """
        db = ServiceFactory.get_service(job.context.target_type)
        manifest = self.get_manifest(job) # Final read of the audit trail
        
        db.execute_query(
            "INSERT INTO job_execution_history (job_id, run_id, rows_in, rows_out, duration) VALUES (%s, %s, %s, %s, %s)",
            (job.id, job.run_id, manifest.raw.total_rows, manifest.write.rows_written, manifest.total_duration)
        )
        
    def _purge_workspace(self, job: "Job"):
        """
        Decision: Immediate reclamation of disk space.
        Deletes the active symlink folder and any remaining stray data.
        """
        if job.folder.exists():
            shutil.rmtree(job.folder)
            
        # Also clean up any 'data/' subfolders that weren't archived
        for step in ["raw", "transform"]:
            physical_data = JOB_STEPS_BASE_DIR / "data" / step / f"{job.id}_*"
            # Logic to glob and delete specifically for this job_id
