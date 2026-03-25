from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

import msgspec
import structlog
from msgspec import field, json

if TYPE_CHECKING:
    from pathlib import Path

    from apps.ingestion.src.core.models.job.status import JobStatus
else:
    from apps.ingestion.src.core.models.job.status import JobStatus

LOG = structlog.getLogger(__name__)


class ErrorPayload(msgspec.Struct):
    step: str
    error_type: str
    message: str
    traceback: str | None = None
    timestamp_utc: str = field(
        default_factory=lambda: datetime.now().astimezone().isoformat()
    )


class BasePayload(msgspec.Struct, kw_only=True):
    commit_hash: str = ""
    source_params: dict[str, Any] = {}
    worker_id: str = ""
    start_timestamp_utc: str = field(
        default_factory=lambda: datetime.now().astimezone().isoformat()
    )

    def save(self, folder_path: Path) -> None:
        """Saves the current state to the standardized manifest file."""
        path = folder_path / "manifest.json"
        with path.open("wb") as f:
            f.write(json.encode(self))

    @classmethod
    def load(cls, folder_path: Path, stage_type: type) -> Any:
        """Loads the manifest and decodes it into a specific Stage type."""
        path = folder_path / "manifest.json"
        with path.open("rb") as f:
            return json.decode(f.read(), type=stage_type)


class ExtractPayload(BasePayload, kw_only=True):
    file_count: int  # Number of files detected
    files: list[str] = []  # List of file paths, include checksum per file
    artifact_folder: Path
    source_row_count: int  # Number of rows detected
    schema_signature: dict[str, str] = {}  # Column names and types
    end_timestamp_utc: str = field(
        default_factory=lambda: datetime.now().astimezone().isoformat()
    )


class TransformPayload(BasePayload, kw_only=True):
    logic_version: str
    transform_type: str
    output_row_count: int  # Number of rows detected
    schema_validation_pass: bool = False  # True if schema matches the expected schema
    refined_schema: dict[str, str] = {}  # Column names and types
    artifact_folder: Path | str
    start_timestamp_utc: str
    end_timestamp_utc: str = field(
        default_factory=lambda: datetime.now().astimezone().isoformat()
    )


class WritePayload(BasePayload, kw_only=True):
    staging_artifact: str
    sink_type: str
    rows_inserted: int
    partition_col: str
    partition_value: str
    sink_identifier: str
    start_timestamp_utc: str
    end_timestamp_utc: str = field(
        default_factory=lambda: datetime.now().astimezone().isoformat()
    )


class AuditPayload(BasePayload, kw_only=True):
    validation_passed: bool
    total_checks_run: int
    failed_checks: dict[str, Any] = {}
    external_app_status: str
    start_timestamp_utc: str
    end_timestamp_utc: str = field(
        default_factory=lambda: datetime.now().astimezone().isoformat()
    )


class PublishPayload(BasePayload, kw_only=True):
    final_destination: str
    final_count: int
    is_idempotent_cleanup_run: bool = False
    start_timestamp_utc: str
    end_timestamp_utc: str = field(
        default_factory=lambda: datetime.now().astimezone().isoformat()
    )


class CompletePayload(BasePayload, kw_only=True):
    # Governance & Privacy
    cleanup_verified: bool  # Confirmation that staging/temp data is purged
    archival_path: str | None  # Path to backup, or None if privacy-restricted
    retention_expiry: str | None  # Date when this log/archive can be deleted
    start_timestamp_utc: str
    end_timestamp_utc: str = field(
        default_factory=lambda: datetime.now().astimezone().isoformat()
    )


class JobManifest(msgspec.Struct, kw_only=True):
    # Top-level Metadata (The "Header")
    job_id: str
    run_id: str
    dataset_id: str
    job_status: JobStatus = JobStatus.PENDING
    current_step: str
    bitmask: int
    retry_count: int = 0

    # Step-Specific Data (The "Body")
    start: BasePayload | None = None
    extract: ExtractPayload | None = None
    transform: TransformPayload | None = None
    write: WritePayload | None = None
    audit: AuditPayload | None = None
    publish: PublishPayload | None = None
    complete: CompletePayload | None = None

    # The "Black Box" Recorder
    error: ErrorPayload | None = None


__sll__ = [
    # BasePayload,
    ExtractPayload,
    TransformPayload,
    WritePayload,
    AuditPayload,
    PublishPayload,
    CompletePayload,
    ErrorPayload,
    JobManifest,
]
