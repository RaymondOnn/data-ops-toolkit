"""Task manifest data structures for stage payloads."""

import msgspec
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from libs.utils.dates import current_timestamp
from msgspec import field


class FileInfo(msgspec.Struct):
    """Metadata for a physical file artifact."""

    path: str
    checksum: str
    row_count: int
    size_bytes: int


class StagePayload(msgspec.Struct, kw_only=True):
    """Base payload with timestamps."""

    start_time: str = field(
        default_factory=lambda: current_timestamp(naive=True).isoformat(sep=" ")
    )
    end_time: str = field(
        default_factory=lambda: current_timestamp(naive=True).isoformat(sep=" ")
    )


class StartPayload(StagePayload):
    """Start stage payload - tracks task initialization."""

    commit_hash: str = ""
    # worker_id: str = ""


class ExtractPayload(StagePayload):
    """Extract stage results."""

    file_count: int = 0
    files: list[FileInfo] = []
    artifact_folder: str = ""
    source_files: list[str] = []
    resource: str | None = None
    source_count: int = 0
    schema: dict[str, str] = {}  # column → type


class TransformPayload(StagePayload):
    """Transform stage results."""

    transform_type: str
    artifact_folder: str | None = None
    output_count: int = 0
    schema_valid: bool = False
    output_schema: dict[str, str] = {}


class WritePayload(StagePayload):
    """Write stage results."""

    staging_artifact: str
    sink_type: str
    write_count: int
    partition_by: str
    partition_value: str
    destination: str


class PublishPayload(StagePayload):
    """Publish stage results."""

    final_path: str
    final_count: int
    start_time: str


class ArchivePayload(StagePayload):
    """Archive stage results."""

    # cleanup_done: bool
    archive_path: str | None
    retention_expiry: str | None


class ErrorInfo(msgspec.Struct):
    """Error information for failed stages."""

    stage: str
    error_type: str
    message: str
    traceback: str | None = None
    timestamp: str = field(
        default_factory=lambda: current_timestamp(naive=True).isoformat(sep=" ")
    )


class TaskManifest(msgspec.Struct, kw_only=True):
    """Complete task state on disk."""

    # Identity
    job_id: str
    run_id: str
    dataset_id: str

    # State
    status: ExecutionStatus
    current_stage: str
    bitmask: int
    retry_count: int = 0
    remarks: str | None = None

    # Stage payloads
    start: StartPayload | None = None
    extract: ExtractPayload | None = None
    transform: TransformPayload | None = None
    write: WritePayload | None = None
    publish: PublishPayload | None = None
    archive: ArchivePayload | None = None

    # Error
    error: ErrorInfo | None = None

    @property
    def is_complete(self) -> bool:
        """Check if all stages have completed."""
        return all(
            getattr(self, stage)
            for stage in ["extract", "transform", "write", "publish", "archive"]
        )
