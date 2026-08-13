from libs.utils.dates import current_timestamp
from msgspec import Struct, field


class BasePayload(Struct, kw_only=True, tag_field="stage"):
    """Base payload with timestamps."""

    step_id: str
    # stage: Stage
    start_time: str = field(
        default_factory=lambda: current_timestamp(naive=True).isoformat(sep=" ")
    )
    end_time: str = field(
        default_factory=lambda: current_timestamp(naive=True).isoformat(sep=" ")
    )


class ErrorInfo(Struct, kw_only=True):
    """Error information for failed stages."""

    step_id: str
    stage: str
    error_type: str
    message: str
    traceback: str | None = None
    timestamp: str = field(
        default_factory=lambda: current_timestamp(naive=True).isoformat(sep=" ")
    )
