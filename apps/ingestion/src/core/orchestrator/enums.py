
import msgspec
import time



class JobMetadata(msgspec.Struct):
    """Typed metadata for a job in the engine queue."""

    job_id: str
    run_id: str
    dataset_id: str
    run_date: str
    config_file: str
    current_step: str
    status: str = "PENDING"
    last_hb: float = msgspec.field(default_factory=time.time)
    retry_count: int = 0
    expires_at: float | None = None
