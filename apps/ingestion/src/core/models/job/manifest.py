from datetime import datetime
from typing import Any

import msgspec
from apps.ingestion.src.core.models.job.status import ExecutionStatus
from msgspec import field


class FileInfo(msgspec.Struct):
    """
    Metadata for an individual physical file artifact.
    """

    path: str  # Absolute or relative path to the parquet file
    checksum: str  # MD5/SHA hash for forensic integrity
    row_count: int  # Number of rows in THIS specific file
    size_bytes: int  # Physical file size on disk


class ErrorPayload(msgspec.Struct):
    stage: str
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


class ExtractPayload(msgspec.Struct, kw_only=True):
    file_count: int = 0  # Number of files detected
    files: list[FileInfo] = []  # List of file metadata
    artifact_folder: str = ""
    source_row_count: int = 0  # Number of rows detected
    schema_signature: dict[str, str] = {}  # Column names and types
    start_timestamp_utc: str = ""
    end_timestamp_utc: str = field(
        default_factory=lambda: datetime.now().astimezone().isoformat()
    )


class TransformPayload(msgspec.Struct, kw_only=True):
    logic_version: str
    transform_type: str
    output_row_count: int  # Number of rows detected
    schema_validation_pass: bool = False  # True if schema matches the expected schema
    refined_schema: dict[str, str] = {}  # Column names and types
    artifact_folder: str
    start_timestamp_utc: str
    end_timestamp_utc: str = field(
        default_factory=lambda: datetime.now().astimezone().isoformat()
    )


class WritePayload(msgspec.Struct, kw_only=True):
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


class AuditPayload(msgspec.Struct, kw_only=True):
    validation_passed: bool
    total_checks_run: int
    failed_checks: dict[str, Any] = {}
    external_app_status: str
    start_timestamp_utc: str
    end_timestamp_utc: str = field(
        default_factory=lambda: datetime.now().astimezone().isoformat()
    )


class PublishPayload(msgspec.Struct, kw_only=True):
    final_destination: str
    final_count: int
    is_idempotent_cleanup_run: bool = False
    start_timestamp_utc: str
    end_timestamp_utc: str = field(
        default_factory=lambda: datetime.now().astimezone().isoformat()
    )


class CompletePayload(msgspec.Struct, kw_only=True):
    # Governance & Privacy
    cleanup_verified: bool  # Confirmation that staging/temp data is purged
    archival_path: str | None  # Path to backup, or None if privacy-restricted
    retention_expiry: str | None  # Date when this log/archive can be deleted
    start_timestamp_utc: str
    end_timestamp_utc: str = field(
        default_factory=lambda: datetime.now().astimezone().isoformat()
    )


class TaskManifest(msgspec.Struct, kw_only=True):
    # Top-level Metadata (The "Header")
    job_id: str
    run_id: str
    dataset_id: str
    status: ExecutionStatus = ExecutionStatus.PENDING
    current_stage: str
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
    FileInfo,
    ExtractPayload,
    TransformPayload,
    WritePayload,
    AuditPayload,
    PublishPayload,
    CompletePayload,
    ErrorPayload,
    TaskManifest,
]
