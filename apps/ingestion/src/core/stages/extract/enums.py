from msgspec import Struct, field

from src.core.stages.contracts.payload import BasePayload


class FileInfo(Struct):
    """Metadata for a physical file artifact."""

    path: str
    checksum: str
    row_count: int
    size_bytes: int
    min_checkpoint: str | None = None
    max_checkpoint: str | None = None


class PartitionExtracted(Struct, kw_only=True):
    """Metadata for a specific date partition extraction."""

    partition_date: str
    row_processed: int = 0
    file_count: int = 0
    source_files: list[str] = field(default_factory=list)
    files: list[FileInfo] = field(default_factory=list)
    checkpoint_start: str
    checkpoint_end: str
    checkpoint_type: str
    checkpoint_state_payload: str = ""
    resource: str | None = None


class ExtractPayload(BasePayload, kw_only=True, tag="extract"):
    """Extract stage results."""

    artifact_folder: str = ""
    rows_processed: int = 0
    output_schema: dict[str, str] = field(default_factory=dict)

    # Granular partition breakdown
    partitions: dict[str, PartitionExtracted] = field(default_factory=dict)
