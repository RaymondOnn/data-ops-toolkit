from msgspec import Struct, field

from src.core.stages.contracts.payload import BasePayload


class PartitionTransformed(Struct, kw_only=True):
    partition_date: str
    rows_in: int = 0  # Pre-transform input rows
    rows_out: int = 0  # Post-transform output rows
    resource: str | None = None  # Inherited source lineage (S3 URI, DB origin, etc.)
    files: list[str] = field(default_factory=list)
    status: str = "SKIPPED"  # SUCCESS, SKIPPED, FAILED
    execution_time_ms: float = 0.0


class TransformPayload(BasePayload, kw_only=True, tag="transform"):
    """Transform stage results."""

    artifact_folder: str | None = None
    rows_processed: int = 0
    # schema_valid: bool = False
    output_schema: dict[str, str] = field(default_factory=dict)

    # Granular partition breakdown tailored for transformations
    partitions: dict[str, PartitionTransformed] = field(default_factory=dict)
