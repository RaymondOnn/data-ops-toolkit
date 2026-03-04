import logging
import socket
import time
import traceback
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional, Type, Literal
from datetime import datetime

import polars as pl
import msgspec
from msgspec import Struct

from src.core.transform.base import TransformFactory
from src.core.ingest.base import IngestionFactory
from src.core.load.base import LoadFactory
from src.core.entities.job.manifest import (
    JobManifest, 
    BasePayload, 
    RawPayload, 
    TransformPayload,
    WritePayload, 
    AuditPayload, 
    PublishPayload,
    CompletePayload,
    ErrorPayload 
)
LOG = logging.getLogger(__name__)
JOB_STEPS_BASE_DIR = Path("~/.ingestion_engine/data/")


class JobBitmask:
    START = "0000001"
    RAW = "0000010"
    TRANSFORM = "0000100"
    WRITE = "0001000"
    AUDIT = "0010000"
    PUBLISH = "0100000"
    COMPLETE = "1000000"


_JOB_ORDER = [
    "start",
    "raw",
    "transform",
    "write",
    "audit",
    "publish",
    "complete",
]

class JobStep(ABC):
    """Base class for JobStage classes."""

    @property
    def bitmask(self) -> str:
        raise NotImplementedError("Subclasses must implement this property")
    
    @property
    def name(self) -> str:
        raise NotImplementedError("Subclasses must implement this property")

    @abstractmethod
    def execute(self, df: Optional[Any] = None) -> Any:
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
        next_step = _JOB_ORDER[idx + 1]
        job.set_step(JobStep.get_step_class_by_name(next_step)())
        return next_step

    def read_parquet_source(self, source_folder: str) -> pl.LazyFrame:
        """Standardized way to lazily load the previous stage's data."""
        path = Path(source_folder)
        if not path.exists():
            raise FileNotFoundError(f"Source folder {source_folder} missing.")
        # scan_parquet is lazy; it doesn't load data into RAM yet
        return pl.scan_parquet(path / "*.parquet")

    def sink_to_parquet(self, lf: pl.LazyFrame, destination: Path) -> dict:
        """
        Executes the lazy plan and sinks to a single parquet file.
        Returns metadata for the payload.
        """
        destination.mkdir(parents=True, exist_ok=True)
        output_file = destination / "part_0000.parquet"
        
        # sink_parquet is the most memory-efficient way to write in Polars
        lf.sink_parquet(output_file, compression="snappy")
        
        # Now we collect just the schema and count (metadata only)
        final_meta = lf.select([
            pl.len().alias("count"),
        ]).collect()

        return {
            "path": str(output_file),
            "rows": final_meta["count"][0],
            "schema": {k: str(v) for k, v in lf.schema.items()}
        }

    def finalize(
        self, 
        job: "Job", 
        results: dict[str, Any] | None = None, 
        exception: Exception | None= None):        
        # 1. READ & BOOTSTRAP
        # Check if file exists and has content
        if not job.folder:
            raise ValueError("Job folder is not set.")
        
        manifest_path = job.folder / "manifest.json"
        if manifest_path.exists() and manifest_path.stat().st_size > 0:
            with open(manifest_path, "rb") as f:
                data = msgspec.json.decode(f.read())
        else:
            # File is empty or doesn't exist: Start Phase
            data = {
                "job_id": job.id,
                "run_id": job.run_id,
                "dataset_name": job.job_config.dataset_name,
                # "status": "PENDING",
                "current_step": "init"
            }

        # 2. MUTATE (Same as before)
        if exception:
            data["status"] = "FAILED"
            data["error"] = {
                "step": self.name,
                "error_type": type(exception).__name__,
                "message": str(exception),
                "traceback": traceback.format_exc()
            }
        else:
            # data["status"] = "RUNNING"
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

    def prepare_stage(self, job: "Job"):
        """Pivots the job folder to the current step's directory."""
        # ~/.ingestion_engine/data/<job_id>/<run_id>/<step_name>/
        job.folder = Path(JOB_STEPS_BASE_DIR).expanduser() / job.id / job.run_id / self.name
        job.folder.mkdir(parents=True, exist_ok=True)
        # The manifest always sits one level ABOVE the stage folders for global access
        job.manifest_path = job.folder.parent / "manifest.json"

    def get_manifest(self, job: "Job") -> JobManifest:
        """Helper to read the current state of the world."""
        if not job.manifest_path.exists():
            raise FileNotFoundError(f"Manifest missing at {job.manifest_path}")
        
        with open(job.manifest_path, "rb") as f:
            return msgspec.json.decode(f.read(), type=JobManifest)
        
    @classmethod
    def get_step_class_by_name(cls, name: str) -> Type["JobStep"]:
        steps = {
            "start": StartStep,
            "raw": RawStep,
            "transform": TransformStep,
            "audit": AuditStep,
            "load": WriteStep,
            "load": PublishStep,
            "complete": CompleteStep,
        }
        if name not in steps:
            raise ValueError(f"Unknown step name: {name}")
        return steps[name]


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
        job.folder = Path(JOB_STEPS_BASE_DIR).expanduser() / self.name /job.id / job.run_id
        job.folder.mkdir(parents=True, exist_ok=True)
        
        # 2. Create the empty placeholder (The "Signal")
        # This satisfies your requirement for finalize() to see an empty file
        manifest_path = job.folder / "manifest.json"
        manifest_path.touch()
            
        # persist job-start metadata using engine helper
        try:
            # 3. Gather System Metadata
            start_timestamp = datetime.now().isoformat()
            commit_hash = self._get_commit_hash() # Use the helper above
            worker_id = f"{socket.gethostname()}-{os.getpid()}"
            
            # 4. Create Payload
            payload = BasePayload(
                step_outcome="COMPLETED",
                commit_hash=commit_hash,
                source_params = {},
                worker_id=worker_id,
                start_timestamp=start_timestamp
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
            return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode('ascii').strip()
        except Exception:
            return "unknown"

class FileInfo(Struct):
    """
    Metadata for an individual physical file artifact.
    """
    path: str        # Absolute or relative path to the parquet file
    checksum: str    # MD5/SHA hash for forensic integrity
    row_count: int   # Number of rows in THIS specific file
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
        try:
            # 1. Strategy Selection
            # Based on job_config (e.g., job_config.source_type = "s3")
            strategy = IngestionFactory.get_strategy(job.job_config.source_type)
            
            LOG.info(f"Executing raw ingestion using strategy: {job.job_config.source_type}")
            
            # The strategy now yields Polars DataFrames or Iterators
            # to be written to the local folder as parquet
            dest_folder = Path(job.folder) / "raw"
            dest_folder.mkdir(parents=True, exist_ok=True)
            
            # Fetch and convert
            # This logic might happen inside the strategy or here
            extracted_files = strategy.fetch_to_parquet(destination=dest_folder)
            
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
                file_infos.append(FileInfo(
                    path=str(f["path"]),
                    checksum=checksum,
                    row_count=f["rows"],
                    size_bytes=f["path"].stat().st_size
                ))
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
                schema_signature={k: str(v) for k, v in f["schema"].items()}
            )
            
            self.finalize(job, results=msgspec.to_builtins(payload))
            
            # 4. State Transition
            return self._transit(job)

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
    
    def _merge_schemas(self, schemas: list[dict]) -> dict[str, str]:
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
        start_time = time.perf_counter()
        self.prepare_stage(job) # Pivots job.folder to .../transform/
        
        try:
            # 1. Access the previous stage's data via Manifest
            manifest = self.get_manifest(job)
            raw_meta = manifest.raw # The IngestedMetadata struct
            
            # 2. Get the Strategy (Custom or Default)
            strategy = TransformFactory.get_strategy(job.id)
            
            # 3. Create the LazyFrame from the Bronze path
            lf = self.read_parquet_source(raw_meta.artifact_path)
            
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
                schema_validation_passed=True, # Strategy could add validation logic
                refined_schema=results["schema"],
                processing_duration_secs=duration_ms
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
            manifest = self.get_manifest(job)
            # 1. Resolve Client & Strategy
            client = DatabaseManager.get_client(job.job_config.destination_type)
            strategy = LoadFactory.get_strategy(job.job_config.load_mode)
            
            # 2. PHASE 1: LOAD TO STAGING
            lf = self.read_parquet_source(manifest.transform.refined_artifact_path)
            staging_results = strategy.load(client, lf, job.job_config.target_destination)
            
            # 3. Finalize Manifest
            payload = WritePayload(
                step_outcome="COMPLETED",
                target_identifier=job.job_config.target_destination,
                load_mode=job.job_config.load_mode,
                sink_type=job.job_config.destination_type,
                staging_artifact=staging_results.get("staging_path") or staging_results.get("staging_table"),
                rows_affected=staging_results["rows"],
                db_connection_id=client.connection_id,
                duration_secs=int((time.perf_counter() - start_time) * 1000)
            )
            
            self.finalize(job, results=payload)
            return self._transit(job)

        except Exception as e:
            self.finalize(job, exception=e)
            raise

class AuditStep(JobStep):
    manifest: AuditPayload
    
    @property
    def bitmask(self) -> str:
        return JobBitmask.AUDIT
    
    @property
    def name(self) -> str:
        return "audit"

    def execute(self, df: Optional[Any] = None) -> None:
        raise NotImplementedError

    def transit(self, job: "Job") -> None:
        job.set_step(WriteStep())

class PublishStep(JobStep):
    manifest: PublishPayload
    
    @property
    def bitmask(self) -> str:
        return JobBitmask.PUBLISH
    
    @property
    def name(self) -> str:
        return "publish"


    def execute(self, df: Optional[Any] = None) -> None:
        LOG.info(f"Loading {len(df)} rows to destination...")
        # Logic for adbc-driver to write to DB would go here
        pass
        return None

    def transit(self, job: "Job") -> None:
        job.set_step(CompleteStep())
        
class CompleteStep(JobStep):
    manifest: CompletePayload
    
    @property
    def bitmask(self) -> str:
        return JobBitmask.COMPLETE
    
    @property
    def name(self) -> str:
        return "complete"

    def execute(self, df: Optional[Any] = None) -> None:
        return None


class Job:
    status: JobStatus,
    run_id: str
    _step: JobStep | None = None
    def __init__(
        self,
        job_id: str,
        job_config: Any,
        job_folder: Optional[Path] = None,
        start_step: Optional[str] = "start",
        run_id: Optional[str] = None,
    ) -> None:
        """
        Initialize a Job instance with the given step.

        :param step: The step name to start from (e.g. "raw", "transform", etc.). If None, the job will start from the beginning.
        :type step: str
        """
        self.id = job_id
        if start_step:
            self._step = JobStep._get_step_by_name(start_step)
        self.job_config = job_config
        self.folder = job_folder
        self.metadata = {
            "job_id": self.id,
            "status": self.status,
            "run_id": run_id,
            "dataset": self.job_config.dataset_name,
        }


    def _get_step_by_name(self, name: str) -> Type[JobStep]:
        """Helper method to get a step class type by name."""
        return JobStep.get_step_class_by_name(name)

    @property
    def step(self) -> JobStep:
        if not self._step:
            raise ValueError("Job is not initialized.")
        return self._step

    def set_step(self, step: JobStep) -> None:
        prev_step = self._step
        self._step = step

    def execute(self) -> None:
        if not self._step:
            raise ValueError("Job is not initialized.")

        self._step.execute()
