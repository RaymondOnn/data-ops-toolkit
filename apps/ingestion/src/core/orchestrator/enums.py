import time
from datetime import datetime
from typing import Any, Self

import msgspec

MISFIRE_GRACE_PERIOD_SECS = 3600


class TaskMetadata(msgspec.Struct):
    """Typed metadata for a job in the engine queue."""

    job_id: str
    run_id: str
    dataset_id: str
    partition_date: str
    config_file: str
    current_stage: str
    status: str = "PENDING"
    last_hb: float = msgspec.field(default_factory=time.time)
    retry_count: int = 0
    expires_at: float | None = None
    blocked_by: str | None = None


class JobRecord(msgspec.Struct, kw_only=True):
    """Typed record representing a job definition from the database."""

    JOB_ID: str
    DATASET_ID: str
    SCHEDULED_TIMESTAMP: datetime
    JOB_STATUS: str
    PARTITION_DATE: str | None = None
    CURRENT_STEP: str | None = None
    JOB_BITMASK: int = 0
    IS_SCHEDULED: int = 0
    RETRY_ATTEMPTS: int = 0
    RUN_ID: str | None = None
    START_TIMESTAMP: datetime | None = None
    END_TIMESTAMP: datetime | None = None
    WATCH_FILE_PATH: str | None = None
    RUNTIME_OVERRIDES: dict[str, Any] | None = None
    TRIGGER_TYPE: str = "CRON"
    MISFIRE_GRACE_SECS: int = MISFIRE_GRACE_PERIOD_SECS
    SOURCE_ROW_COUNT: int | None = None
    FINAL_ROW_COUNT: int | None = None
    FINAL_MANIFEST: str | None = None

    def __post_init__(self) -> None:
        """Validation logic can be added here."""
        if not self.JOB_ID or not self.DATASET_ID:
            raise ValueError(f"Invalid JobRecord: Missing ID for {self}")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Factory method to create a JobRecord from a dictionary."""
        return msgspec.convert(data, cls)
