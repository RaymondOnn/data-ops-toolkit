from msgspec import field

from src.core.stages.contracts.payload import BasePayload


class TransformPayload(BasePayload, kw_only=True, tag="transform"):
    """Transform stage results."""

    # transform_type: str
    artifact_folder: str | None = None
    output_count: int = 0
    # schema_valid: bool = False
    output_schema: dict[str, str] = field(default_factory=dict)
    # stage: Stage = Stage.TRANSFORM
