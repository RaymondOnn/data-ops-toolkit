from src.core.stages.contracts.payload import BasePayload


class PublishPayload(BasePayload, kw_only=True, tag="publish"):
    """Publish stage results."""

    final_path: str
    final_count: int
    start_time: str
    # stage: Stage = Stage.PUBLISH
