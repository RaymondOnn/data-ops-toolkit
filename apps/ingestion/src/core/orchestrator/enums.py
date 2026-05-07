import time
from datetime import datetime
from typing import Any, Self

import msgspec
from apps.ingestion.src.core.models.task.enums import TaskRef
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.constants import (
    CACHE_TASK_NAMESPACE,
    MISFIRE_GRACE_PERIOD_SECS,
    STRIP_TZ_FOR_DB,
)
from libs.utils.dates import diff_seconds, standardize_timestamp


def to_ch_datetime(ts: Any) -> str | None:
    """
    Forces input into 'YYYY-MM-DD HH:MM:SS.SSS' format.
    Explicitly removes 'T' and timezone offsets (+08:00, Z)
    to satisfy ClickHouse DateTime64(3) requirements.
    """
    if ts is None or ts == "":
        return None

    try:
        # Leverage standardize_timestamp to handle parsing and naive conversion
        dt = standardize_timestamp(ts, force_naive=STRIP_TZ_FOR_DB)
        return dt.format("YYYY-MM-DD HH:mm:ss.SSS")
    except Exception as e:
        print(f"FAILED_TO_PARSE_TS: {ts} | Error: {e}")
        # Last ditch effort: regex-style strip
        return str(ts).replace("T", " ").split("+")[0].split("Z")[0]


class TaskMetadata(msgspec.Struct):
    """Typed metadata for a job in the task queue."""

    job_id: str
    run_id: str
    dataset_id: str
    partition_date: str
    config_file: str
    current_stage: str
    status: str = "WAITING"
    last_hb: float = msgspec.field(default_factory=time.time)
    retry_count: int = 0
    expires_at: float | None = None
    blocked_by: str | None = None

    @classmethod
    def from_ref(
        cls, ref: "TaskRef", config_file: str, expires_at: float | None = None
    ) -> "TaskMetadata":
        """Standardized factory to create metadata from a reference."""
        return cls(
            job_id=ref.job_id,
            run_id=ref.run_id,
            dataset_id=ref.dataset_id,
            partition_date=ref.partition_date,
            status=ref.status,
            config_file=config_file,
            current_stage=ref.stage,
            last_hb=time.time(),
            expires_at=expires_at,
        )

    def to_ref(self) -> TaskRef:
        """Converts metadata back into a TaskRef identity handle."""
        return TaskRef(
            namespace=CACHE_TASK_NAMESPACE,
            status=self.status,
            stage=self.current_stage,
            job_id=self.job_id,
            dataset_id=self.dataset_id,
            partition_date=self.partition_date,
            run_id=self.run_id,
        )


class JobRecord(msgspec.Struct, kw_only=True):
    """Typed record representing a job definition from the database."""

    JOB_ID: str
    DATASET_ID: str
    SCHEDULED_TIMESTAMP_LC: datetime
    JOB_STATUS: str
    PARTITION_DATE: str | None = None
    CURRENT_STAGE: str | None = None
    JOB_BITMASK: str | None = None
    IS_SCHEDULED: int = 0
    RETRY_ATTEMPTS: int = 0
    RUN_ID: str
    LAST_UPDATED_AT_TS_LC: datetime | None = None
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
        """Normalize state fields and validate identity."""
        if not self.JOB_ID or not self.DATASET_ID:
            raise ValueError(f"Invalid JobRecord: Missing ID for {self}")

        for field in ["CURRENT_STAGE", "JOB_STATUS"]:
            val = getattr(self, field, None)
            if isinstance(val, str):
                super().__setattr__(field, val.upper())

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

        # Safely calculate delay regardless of input types or timezone awareness
        # For ClickHouse-centric apps, we keep it naive.
        # For Aware apps, we would pass timezone=self.exec_ctx.timezone
        delay = diff_seconds(now, self.SCHEDULED_TIMESTAMP_LC, timezone=None)

        return delay > self.MISFIRE_GRACE_SECS


class JobUpdate(msgspec.Struct, kw_only=True):
    """
    Typed subset of columns used for partial state transitions and heartbeats.
    """

    JOB_ID: str
    DATASET_ID: str
    PARTITION_DATE: str
    JOB_STATUS: str
    LAST_UPDATED_AT_TS_LC: str
    RETRY_ATTEMPTS: int = 0
    CURRENT_STAGE: str | None = None
    JOB_BITMASK: str | None = None
    SOURCE_ROW_COUNT: int | None = None
    FINAL_ROW_COUNT: int | None = None
    REMARKS: str | None = None
    FINAL_MANIFEST: str | None = None
    START_TIMESTAMP_LC: str | None = None
    END_TIMESTAMP_LC: str | None = None
    RUNTIME_OVERRIDES: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        """Sanitize all fields for normalization (timestamps and stages)."""
        if isinstance(self.CURRENT_STAGE, str):
            super().__setattr__("CURRENT_STAGE", self.CURRENT_STAGE.upper())

        for name, _ in self.__annotations__.items():
            if "TIMESTAMP" in name or name.endswith("_LC"):
                val = getattr(self, name, None)
                if val is not None:
                    super().__setattr__(name, to_ch_datetime(val))

    def __setattr__(self, name: str, value: Any) -> None:
        """Intercepts assignments to ensure data normalization."""
        if name == "CURRENT_STAGE" and isinstance(value, str):
            value = value.upper()

        if "TIMESTAMP" in name or name.endswith("_LC"):
            value = to_ch_datetime(value)

        super().__setattr__(name, value)
