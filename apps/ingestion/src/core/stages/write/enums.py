from src.core.stages.contracts.payload import BasePayload


class WritePayload(BasePayload, kw_only=True, tag="write"):
    """Write stage results."""

    staging_artifact: str
    sink_type: str
    write_count: int
    partition_on: str
    partition_value: str
    destination: str
    # stage: Stage = Stage.WRITE
