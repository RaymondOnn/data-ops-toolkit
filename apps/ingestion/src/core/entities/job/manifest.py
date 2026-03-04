import logging
import os
from typing import Any, Literal, Optional
from pathlib import Path
from datetime import datetime

import msgspec
from msgspec import Struct, json, field

LOG = logging.getLogger(__name__)

class ErrorPayload(Struct):
    step: str
    error_type: str
    message: str
    traceback: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

class BasePayload(Struct):
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
    artifact_folder: Path
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
    checks_applied: list[str] = []
    row_count_diff: int
    errors: list[str] = []
    
class PublishPayload(BasePayload):
    step_outcome: str
    final_count: int
    published_at_utc: str # When it was published. ISO format
    is_idempotent_cleanup_run: bool = False
    

class CompletePayload(BasePayload):
    final_status: str
    total_duration_secs: float
    end_timestamp: str # ISO format
    cleanup_verified: bool
    archival_path: Path
    retention_expiry: str

class JobManifest(Struct):
    # Top-level Metadata (The "Header")
    job_id: str
    run_id: str
    dataset_name: str
    # job_status: Literal["PENDING", "RUNNING", "COMPLETED", "FAILED"]
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

def atomic_manifest_update(manifest_path: Path, step_name: str, payload: dict[str, Any]) -> None:
    # 1. Load the "Old" Truth
    if manifest_path.exists():
        with open(manifest_path, "rb") as f:
            old_data = msgspec.json.decode(f.read(), type=JobManifest)
            manifest_dict = msgspec.to_builtins(old_data)
    else:
        # Initialize if it's the very first step
        manifest_dict = {"status": "PENDING", "current_step": "init"}

    # 2. Prepare the "New" Truth in memory
    manifest_dict[step_name] = payload
    manifest_dict["current_step"] = step_name
    manifest_dict["status"] = "COMPLETED"

    try:
        # 3. Validation Step (The "Gatekeeper")
        # We convert back to the Struct to ensure types are correct 
        # and no required fields are missing.
        new_manifest_obj = msgspec.convert(manifest_dict, JobManifest)
        new_bytes = msgspec.json.encode(new_manifest_obj)
        
        # 4. Write to a Temporary Buffer
        temp_path = manifest_path.with_suffix(".tmp")
        with open(temp_path, "wb") as f:
            f.write(new_bytes)
            f.flush()
            os.fsync(f.fileno()) # Force the OS to physically write to disk

        # 5. The Atomic Pointer Swap
        # On POSIX (Linux/Mac), this is an atomic operation.
        os.replace(temp_path, manifest_path)
        
    except Exception as e:
        # If anything fails (validation, disk full, etc.), 
        # the original manifest_path is untouched.
        LOG.error(f"Manifest update failed! Original file preserved. Error: {e}")
        raise


def finalize_manifest(manifest_path: Path, step_name: str, payload: dict[str, Any]) -> None:
    """
    The final 'Commit' of a JobStep.
    """
    # 1. READ: Load the previous state
    # If it's the first step, we start fresh.
    if manifest_path.exists():
        with open(manifest_path, "rb") as f:
            # We decode to a dict to allow for flexible merging
            current_data = msgspec.json.decode(f.read())
    else:
        current_data = {}

    # 2. MUTATE: Add the new step payload
    current_data[step_name] = payload
    current_data["current_step"] = step_name
    current_data["status"] = "COMPLETED"
    current_data["last_updated"] = datetime.now().isoformat()

    # 3. VALIDATE: The "Dry Run"
    try:
        # This checks types, required fields, and nesting constraints
        validated_obj = msgspec.convert(current_data, JobManifest)
        final_bytes = msgspec.json.encode(validated_obj)
    except msgspec.ValidationError as e:
        # LOG AND HALT: Do not proceed to swap if data is invalid
        LOG.critical(f"Step {step_name} produced invalid manifest data: {e}")
        raise

    # 4. SWAP: Atomic write
    temp_path = manifest_path.with_suffix(".tmp")
    with open(temp_path, "wb") as f:
        f.write(final_bytes)
        f.flush()
        os.fsync(f.fileno()) # Ensure it's physically written to the platter
    
    # The pointer swap
    os.replace(temp_path, manifest_path)
    
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