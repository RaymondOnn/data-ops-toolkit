from typing import Any, Literal, Optional
from pathlib import Path
from datetime import datetime

import structlog
import msgspec
from msgspec import json, field

from src.core.entities.job.base import JobStatus


LOG = structlog.getLogger(__name__)

class ErrorPayload(msgspec.Struct):
    step: str
    error_type: str
    message: str
    traceback: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

class BasePayload(msgspec.Struct):
    step_outcome: str
    commit_hash: str = ""
    source_params: dict[str, Any] = {}
    worker_id: str = ""
    start_timestamp: str = ""
    
    
    def save(self, folder_path: Path) -> None:
        """Saves the current state to the standardized manifest file."""
        path = folder_path / "manifest.json"
        with open(path, "wb") as f:
            f.write(json.encode(self))

    @classmethod
    def load(cls, folder_path: Path, stage_type: type) -> Any:
        """Loads the manifest and decodes it into a specific Stage type."""
        path = folder_path / "manifest.json"
        with open(path, "rb") as f:
            return json.decode(f.read(), type=stage_type)

# TODO: Zero Byte Check
class RawPayload(BasePayload):
    step_outcome: str
    file_count: int # Number of files detected
    files: list[str] = [] # List of file paths, include checksum per file
    artifact_folder: Path
    raw_row_count: int # Number of rows detected
    schema_signature: dict[str, str] = {} # Column names and types
    


class TransformPayload(BasePayload):
    step_outcome: str
    logic_version: str
    output_row_count: int # Number of rows detected
    schema_validation_pass: bool = False # True if schema matches the expected schema
    refined_schema: dict[str, str] = {} # Column names and types
    artifact_folder: Path | str
    processing_duration_secs: int
    
class WritePayload(BasePayload):
    step_outcome: str
    target_identifier: str
    staging_artifact: str
    sink_type: str
    load_mode: Literal["APPEND", "UPSERT", "OVERWRITE"]
    rows_affected: int
    sink_connection_id: int 
    load_duration_secs: float
    
class AuditPayload(BasePayload):
    step_outcome: str
    validation_passed: bool
    total_checks_run: int
    failed_checks: dict[str, Any] = {}
    external_app_status: str
    audit_duration_ms: int
    
class PublishPayload(BasePayload):
    step_outcome: str
    final_destination: str
    final_count: int
    published_at_utc: str # When it was published. ISO format
    is_idempotent_cleanup_run: bool = False
    

class CompletePayload(BasePayload):
    final_status: str
    end_timestamp: str          # Final ISO-8601 timestamp
    total_duration_secs: float  # Wall-clock time from Raw to Complete
    
    # Governance & Privacy
    cleanup_verified: bool      # Confirmation that staging/temp data is purged
    archival_path: str | None   # Path to backup, or None if privacy-restricted
    retention_expiry: str | None # Date when this log/archive can be deleted

class JobManifest(msgspec.Struct):
    # Top-level Metadata (The "Header")
    job_id: str
    run_id: str
    dataset_name: str
    job_status: JobStatus = JobStatus.PENDING
    current_step: str
    
    # Step-Specific Data (The "Body")
    start: Optional[BasePayload] = None
    raw: Optional[RawPayload] = None
    transform: Optional[TransformPayload] = None
    write: Optional[WritePayload] = None
    audit: Optional[AuditPayload] = None
    publish: Optional[PublishPayload] = None
    complete: Optional[CompletePayload] = None
    
    # The "Black Box" Recorder
    error: Optional[ErrorPayload] = None

    
__sll__ = [
    # BasePayload,
    RawPayload,
    TransformPayload,
    WritePayload,
    AuditPayload,
    PublishPayload,
    CompletePayload,
    ErrorPayload,
    JobManifest
]