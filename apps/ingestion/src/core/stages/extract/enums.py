from msgspec import Struct, field

from src.core.stages.contracts.payload import BasePayload


class FileInfo(Struct):
    """Metadata for a physical file artifact."""

    path: str
    checksum: str
    row_count: int
    size_bytes: int


class ExtractPayload(BasePayload, kw_only=True, tag="extract"):
    """Extract stage results."""

    file_count: int = 0
    files: list[FileInfo] = field(default_factory=list)
    artifact_folder: str = ""
    source_files: list[str] = field(default_factory=list)
    resource: str | None = None
    source_count: int = 0
    schema: dict[str, str] = field(default_factory=dict)  # column → type
    # stage: Stage = Stage.EXTRACT
