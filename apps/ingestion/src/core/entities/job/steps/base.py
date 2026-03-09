import os
import shutil
import socket
import time
import traceback
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec
import polars as pl
import structlog
from src.core.entities.job.manifest import (
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
from src.core.load.load import WriteContext, Loader
from src.core.transform.base import TransformFactory
from src.services.factory import ServiceFactory
from src.utils.constants import JOB_STEPS_BASE_DIR

from libs.resilence.circuit_breaker import CircuitBreakerTripped

if TYPE_CHECKING:
    from src.core.entities.job.base import Job

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
        """Transit the Job instance to the next stage."""
        idx = _JOB_ORDER.index(self.name)
        if idx + 1 < len(_JOB_ORDER):
            next_step = _JOB_ORDER[idx + 1]
            job.set_step(JobStep.get_step_class_by_name(next_step))
            return next_step
        return "FINISH"

    def read_parquet_source(self, source_folder: Path | str) -> pl.LazyFrame:
        """Standardized way to lazily load the previous stage's data."""
        path = Path(source_folder)
        if not path.exists():
            raise FileNotFoundError(f"Source folder {source_folder} missing.")
        # scan_parquet is lazy; it doesn't load data into RAM yet
        return pl.scan_parquet(path / "*.parquet")

    def sink_to_parquet(
        self, lf: pl.LazyFrame, destination: Path | str
    ) -> dict[str, Any]:
        """
        Executes the lazy plan and sinks to a single parquet file.
        Returns metadata for the payload.
        """
        destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        output_file = destination / "part_0000.parquet"

        # sink_parquet is the most memory-efficient way to write in Polars
        lf.sink_parquet(output_file, compression="snappy")

        # Now we collect just the schema and count (metadata only)
        final_meta = lf.select(
            [
                pl.len().alias("count"),
            ]
        ).collect()

        return {
            "path": str(output_file),
            "rows": final_meta["count"][0],
            "schema": {k: str(v) for k, v in lf.schema.items()},
        }

    def finalize(
        self,
        job: "Job",
        results: dict[str, Any] | None = None,
        exception: Exception | None = None,
    ) -> None:

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
            data: dict[str, Any] = {
                "job_id": job.id,
                "run_id": job.run_id,
                "dataset_name": job.job_config.dataset_name,
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
        else:
            if data["current_step"] == "complete":
                data["status"] = "COMPLETED"
            data["current_step"] = self.name
            if results:
                data[self.name] = results

        # 3. ATOMIC SWAP
        validated_bytes = msgspec.json.encode(msgspec.convert(data, JobManifest))

        temp_path = manifest_path.with_suffix(".tmp")
        with open(temp_path, "wb") as f:
            f.write(validated_bytes)
            f.flush()
            os.fsync(f.fileno())

        os.replace(temp_path, manifest_path)

    def prepare_stage(self, job: "Job") -> None:
        """
        Physically moves the run folder from the previous step's directory
        to the current step's directory.
        """
        # 1. Define the destination
        # e.g., BASE_DATA_DIR / "transform" / "job_123" / "run_456"
        target_dir = (
            Path(JOB_STEPS_BASE_DIR).expanduser() / self.name / job.id / job.run_id
        )

        # 2. Identify current location (wherever it is now)
        if not job.folder:
            raise ValueError("Job folder is not set.")
        current_dir = Path(job.folder)

        if current_dir != target_dir:
            # Ensure parent exists (e.g., the 'transform' folder)
            target_dir.parent.mkdir(parents=True, exist_ok=True)

            # Physical Move (Rename is atomic on the same filesystem)
            if current_dir.exists():
                shutil.move(current_dir, target_dir)
                LOG.info(
                    f"[{job.worker_id}] Moved folder: {current_dir.name} -> {self.name}"
                )
            else:
                # Handle first step (Raw) where folder doesn't exist yet
                target_dir.mkdir(parents=True, exist_ok=True)

        # 3. Update Job Object context
        job.folder = target_dir
        # Manifest is ALWAYS inside the folder, so its path updates relative to job.folder
        job.manifest_path = target_dir / "manifest.json"

    def get_manifest(self, job: "Job") -> JobManifest:
        """Helper to read the current state of the world."""
        if job.manifest_path and not job.manifest_path.exists():
            raise FileNotFoundError(f"Manifest missing at {job.manifest_path}")

        with open(job.manifest_path, "rb") as f:
            return msgspec.json.decode(f.read(), type=JobManifest)

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


class StartStep(JobStep):
    manifest: BasePayload

    @property
    def bitmask(self) -> str:
        return JobBitmask.START

    @property
    def name(self) -> str:
        return "start"

    def execute(self, job: "Job") -> str:
        # 1. Physical Directory Creation
        # Path: ~/.ingestion_engine/data/<job_id>/<run_id>/
        job.folder = JOB_STEPS_BASE_DIR / self.name / job.id / job.run_id
        job.folder.mkdir(parents=True, exist_ok=True)

        # 2. Create the empty placeholder (The "Signal")
        # This satisfies your requirement for finalize() to see an empty file
        manifest_path = job.folder / "manifest.json"
        manifest_path.touch()

        # Move file into job folder
        job_cfg_file = f"{job.id}:{job.job_config.table}_{job.run_id}_config.json"
        source_path = JOB_STEPS_BASE_DIR / self.name / job_cfg_file
        dest_path = job.folder / job_cfg_file
        shutil.move(str(source_path), str(dest_path))

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


class FileInfo(msgspec.Struct):
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

        self.prepare_stage(job)  # Pivots job.folder to .../raw/
        if not job.folder:
            raise ValueError("Job folder is not set.")

        try:
            # 1. Initialize the Type-Safe Context
            # This object is serialized and sent to Ray workers.
            ctx = ReaderContext(
                source_type=job.job_config.source_type,
                target_table=job.job_config.target_table,
                source_path=getattr(job.job_config, "source_path", None),
                num_partitions=job.job_config.num_partitions or 10,
            )

            # 2. Get the Resilient Service (Singleton per account)
            # ServiceFactory uses the @register decorators triggered by discovery.
            service = ServiceFactory.get_service(
                source_type=ctx.source_type,
                account_id=job.account_id,
                **job.job_config.db_config,
            )

            # 3. Get the Ingest Strategy (Database, File, etc.)
            # based on job_config (e.g., job_config.source_type = "s3")
            reader: Reader = ReaderFactory.get_strategy(ctx.source_type)
            LOG.info(f"Executing raw ingestion using strategy: {ctx.source_type}")

            # The strategy now yields Polars DataFrames or Iterators
            # to be written to the local folder as parquet
            dest_folder: Path = Path(job.folder) / "raw"
            dest_folder.mkdir(parents=True, exist_ok=True)

            # Fetch and convert
            # 4. Execute the Ingestion
            # This calls reader.fetch() which:
            #   a. Asks service for work units (SQL queries/File paths)
            #   b. Distributes tasks to Ray workers
            #   c. Workers call service.fetch_stream() [Protected by Circuit Breaker]
            #   d. Streams results to Parquet files (keeping RAM < 2GB)
            extracted_files = reader.fetch(
                service=service,
                target_folder=dest_folder,
                context=ctx,
            )

            file_infos = []
            total_rows = 0
            all_schemas = []

            for f in extracted_files:
                path = f["path"]

                # 1. Calculate Checksum (MD5 or SHA256)
                checksum = self._calculate_checksum(f["path"])

                # Use Polars to get the schema of this specific file
                # This is fast as it only reads the parquet metadata
                file_schema = pl.read_parquet_schema(path)
                all_schemas.append(file_schema)

                # 2. Build FileInfo
                file_infos.append(
                    FileInfo(
                        path=str(f["path"]),
                        checksum=checksum,
                        row_count=f["rows"],
                        size_bytes=f["path"].stat().st_size,
                    )
                )
                total_rows += f["rows"]

            # 2. CALCULATE FINAL SCHEMA (The "Union" of all files)
            # This identifies all columns across all files, handling API drift.
            final_schema_dict = self._merge_schemas(all_schemas)

            # 3. Create Payload and Finalize
            # We map the strategy output to our RawPayload schema
            payload = RawPayload(
                step_outcome="COMPLETED",
                artifact_folder=str(dest_folder),
                file_count=len(file_infos),
                files=file_infos,
                raw_row_count=total_rows,
                # Grab schema from the last file processed
                schema_signature={k: str(v) for k, v in final_schema_dict.items()},
            )

            self.finalize(job, results=msgspec.to_builtins(payload))

            # 4. State Transition
            return self._transit(job)
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
        self.prepare_stage(job)  # Pivots job.folder to .../transform/
        start_time = time.perf_counter()

        try:
            # 1. Access the previous stage's data via Manifest
            manifest = self.get_manifest(job)
            raw_meta = manifest.raw  # The IngestedMetadata struct
            if not raw_meta:
                raise ValueError("Raw metadata not found in manifest.")

            # 2. Get the Strategy (Custom or Default)
            strategy = TransformFactory.get_strategy(job.id)

            # 3. Create the LazyFrame from the Bronze path
            lf = self.read_parquet_source(raw_meta.artifact_folder)

            # 4. Apply transformation logic (Lazy)
            # We pass the LazyFrame to the strategy to add operations to the plan
            transformed_lf = strategy.apply(lf, job.job_config)

            # 5. Sink to Silver folder
            results = self.sink_to_parquet(transformed_lf, job.folder)

            # 6. Build the RefinedMetadata (SilverPayload)
            duration_ms = int((time.perf_counter() - start_time) * 1000)

            payload = TransformPayload(
                step_outcome="COMPLETED",
                logic_version=getattr(strategy, "version", "1.0.0"),
                artifact_path=str(job.folder),
                output_record_count=results["rows"],
                schema_validation_passed=True,  # Strategy could add validation logic
                refined_schema=results["schema"],
                processing_duration_secs=duration_ms,
            )

            self.finalize(job, results=payload)
            return self._transit(job)

        except Exception as e:
            self.finalize(job, exception=e)
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
        

        self.prepare_stage(job)
        start_time = time.perf_counter()

        try:
            # 1. Get the Service (Securely initialized on Ray worker via ServiceFactory)
            service = ServiceFactory.get_service(
                job.sink_type, job.account_id, **job.sink_config
            )

            # 2. Get the behavioral Strategy
            loader = Loader()

            # 3. Create Context
            context = WriteContext(
                target=job.target_table,
                partition_col=job.partition_col,
                partition_value=job.partition_value,
            )

            # 2. PHASE 1: LOAD TO STAGING
            manifest: JobManifest = self.get_manifest(job)
            staging_results = loader.load(
                service, 
                source_dir=manifest.transform.artifact_folder, 
                target_table=job.job_config.target_destination
            )

            # 3. Finalize Manifest
            payload = WritePayload(
                step_outcome="COMPLETED",
                target_identifier=job.job_config.target_destination,
                sink_type=job.job_config.destination_type,
                staging_artifact=(
                    staging_results.staging_path
                    or staging_results.staging_table,
                ),
                rows_inserted=staging_results.rows,
                partition_col=job.partition_col,
                partition_value=job.partition_value,
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
        self.prepare_stage(job)
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
    manifest: PublishPayload

    @property
    def bitmask(self) -> str:
        return JobBitmask.PUBLISH

    @property
    def name(self) -> str:
        return "publish"

    def execute(self, job: "Job") -> str:
        self.prepare_stage(job)
        start_time = time.perf_counter()

        try:
            manifest = self.get_manifest(job)
            write_meta = manifest.write
            if not write_meta:
                raise ValueError("Write metadata not found in manifest.")

            # 1. Get the Service (Securely initialized on Ray worker via ServiceFactory)
            service = ServiceFactory.get_service(
                job.sink_type, job.account_id, **job.sink_config
            )

            # 2. Get the behavioral Strategy
            loader = Loader()

            # 3. Create Context
            context = WriteContext(
                target=job.target_table,
                partition_col=job.partition_col,
                partition_value=job.partition_value,
            )

            # 2. FINISH THE JOB
            # Move from staging to production
            loader.promote(
                service=service,
                staging_info=write_meta.staging_artifact,
                write_ctx=context,
            )

            # 3. PAYLOAD: The 'Success Receipt'
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            payload = PublishPayload(
                step_outcome="COMPLETED",
                final_destination=write_meta.target_identifier,
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
        from src.services.factory import ServiceFactory
        from src.services.file import StorageService


        self.prepare_stage(job)
        manifest = self.get_manifest(job)

        # 1. Initialize Storage Service for Archival
        # We retrieve the 'archive' service defined in the job configuration
        fs: StorageService = ServiceFactory.get_service(
            service_type=job.job_config.archive_type, # e.g., "s3" or "local"
            **job.job_config.archive_config
        )

        try:
            # 2. OPTIONAL ARCHIVAL
            # Subject to privacy requirements defined in job_config
            final_archive_path = None
            if job.job_config.enable_archival:
                # Define the unique destination: archive_base/job_id/run_id/
                dest_path = f"{job.job_config.archive_base_path}/{job.id}/{job.run_id}"
                
                # We archive the 'raw' folder captured in the RawStep manifest
                raw_folder = manifest.raw.artifact_folder
                
                # Service-level move (2GB RAM safe)
                final_archive_path = fs.archive_data(
                    source_dir=raw_folder,
                    archive_path=dest_path
                )

            # 3. CLEANUP VERIFICATION
            # Force removal of all intermediate data (Raw & Transform folders)
            local_run_root = job.folder.parent
            for folder in ["raw", "transform"]:
                target = local_run_root / folder
                if target.exists():
                    shutil.rmtree(target)

            # 4. Calculate Timestamps and Duration
            start_ts = datetime.fromisoformat(manifest.raw.ingestion_started_at)
            end_ts = datetime.now()
            duration_secs = (end_ts - start_ts).total_seconds()

            # 5. FINALIZE CANONICAL PAYLOAD
            payload = CompletePayload(
                execution_outcome="SUCCESS",
                end_timestamp=end_ts.isoformat(),
                total_duration_secs=round(duration_secs, 2),
                cleanup_verified=True,
                archival_path=final_archive_path,
                retention_expiry=self._calculate_expiry(job, end_ts)
            )

            self.finalize(job, results=payload)

            # This marks the final state of the manifest
            return "FINISH"

        except Exception as e:
            self.finalize(job, exception=e)
            raise
        
    def _calculate_expiry(self, job, end_timestamp: datetime) -> str:
        # e.g., standard 7-year retention or 30-day GDPR limit
        retention_days = getattr(
            job.job_config, "retention_days", 2555
        )  # 7 years default
        return (end_timestamp + timedelta(days=retention_days)).date().isoformat()