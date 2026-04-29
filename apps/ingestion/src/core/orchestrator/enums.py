import time
from datetime import datetime
from typing import Any, Self

import msgspec
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.constants import APP_TIMEZONE_LC, MISFIRE_GRACE_PERIOD_SECS


class TaskMetadata(msgspec.Struct):
    """Typed metadata for a job in the task queue."""

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
    SCHEDULED_TIMESTAMP_LC: datetime
    JOB_STATUS: str
    PARTITION_DATE: str | None = None
    CURRENT_STEP: str | None = None
    JOB_BITMASK: int = 0
    IS_SCHEDULED: int = 0
    RETRY_ATTEMPTS: int = 0
    RUN_ID: str
    START_TIMESTAMP_LC: datetime | None = None
    END_TIMESTAMP_LC: datetime | None = None
    WATCH_FILE_PATH: str | None = None
    RUNTIME_OVERRIDES: dict[str, Any] | None = None
    TRIGGER_TYPE: str = "CRON"
    MISFIRE_GRACE_SECS: int = MISFIRE_GRACE_PERIOD_SECS
    SOURCE_ROW_COUNT: int | None = None
    FINAL_ROW_COUNT: int | None = None
    FINAL_MANIFEST: str | None = None
    IS_SNAPSHOT: bool = False
    EXPIRATION_THRESHOLD: datetime | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Factory method to create a JobRecord from a dictionary."""
        return msgspec.convert(data, cls)

    def __post_init__(self) -> None:
        """Validation logic can be added here."""
        if not self.JOB_ID or not self.DATASET_ID:
            raise ValueError(f"Invalid JobRecord: Missing ID for {self}")

    @property
    def is_expired(self) -> bool:
        """Internal signal derived from DB-calculated threshold."""
        if not self.IS_SNAPSHOT or not self.EXPIRATION_THRESHOLD:
            return False

        threshold = self.EXPIRATION_THRESHOLD
        if threshold.tzinfo is None:
            threshold = threshold.replace(tzinfo=APP_TIMEZONE_LC)

        now = datetime.now(threshold.tzinfo)
        return now > threshold

    @property
    def has_been_triggered(self) -> bool:
        """
        Indicates if the Orchestrator has started the provisioning process.

        A 'PENDING' status implies the record is an untriggered intent.
        Any status like PROVISIONED, QUEUED, or RUNNING implies that
        physical artifacts (folders/configs) have been created.
        """
        return self.JOB_STATUS not in [
            ExecutionStatus.PENDING.value,
            ExecutionStatus.UNKNOWN.value,
        ]

    def is_misfired(self, now: datetime) -> bool:
        """
        Checks if the job has missed its allowed execution window (grace period).

        Misfire Policy is handled here based on GRACE_PERIOD_SECS:
        -1: Fire Immediately (Always True)
        0 : Skip (Always False if delay > 0)
        >0: Grace Period (True if within bounds)
        """
        if self.MISFIRE_GRACE_SECS == -1:
            return False

        sched = self.SCHEDULED_TIMESTAMP_LC
        if sched.tzinfo is None:
            # Assumption: DB timestamps are localized or UTC relative to system
            sched = sched.replace(tzinfo=now.tzinfo)

        delay = (now - sched).total_seconds()
        return delay > self.MISFIRE_GRACE_SECS
