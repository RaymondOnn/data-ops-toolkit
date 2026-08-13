from src.core.stages.contracts.payload import BasePayload


class ArchivePayload(BasePayload, kw_only=True, tag="archive"):
    """Archive stage results."""

    # cleanup_done: bool
    archive_path: str | None
    retention_expiry: str | None
    # stage: Stage = Stage.ARCHIVE
