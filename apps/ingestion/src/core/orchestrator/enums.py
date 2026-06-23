"""Core enums and data structures for task orchestration state management."""

import time
from datetime import datetime
from typing import Any, Self

import msgspec
from apps.ingestion.src.core.models.task.enums import TaskRef
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.constants import (
    CACHE_TASK_NAMESPACE,
    STRIP_TZ_FOR_DB,
)
from libs.utils.dates import parse_timestamp
from loguru import logger

LOG = logger


# =============================================================================
# Helper Functions
# =============================================================================


def to_ch_datetime(ts: Any) -> str | None:
    """
    Convert timestamp to ClickHouse DateTime64(3) format.

    Returns 'YYYY-MM-DD HH:MM:SS.SSS' without timezone.
    """
    if not ts:
        return None

    try:
        dt = parse_timestamp(ts, naive=STRIP_TZ_FOR_DB)
        return dt.format("YYYY-MM-DD HH:mm:ss.SSS")
    except Exception:
        # Fallback: brute-force strip
        return str(ts).replace("T", " ").split("+")[0].split("Z")[0]


# =============================================================================
# TaskMetadata - Hot Cache Entry
# =============================================================================


class TaskMetadata(msgspec.Struct):
    """
    Runtime metadata for a task in the hot cache.

    Persisted on disk to survive orchestrator restarts.
    """

    job_id: str
    run_id: str
    dataset_id: str
    partition_date: str
    config_file: str
    current_stage: str
    status: str = "WAITING"
    last_hb: float = msgspec.field(default_factory=time.time)
    next_attempt_ts: str | None = None
    retry_count: int = 0
    rewind_history: dict[str, str] = msgspec.field(default_factory=dict)
    expires_at: float | None = None
    blocked_by: str | None = None

    @classmethod
    def from_ref(
        cls, ref: "TaskRef", config_file: str, expires_at: float | None = None
    ) -> "TaskMetadata":
        """Create metadata from a TaskRef."""
        return cls(
            job_id=ref.identity.job_id,
            run_id=ref.identity.run_id,
            dataset_id=ref.identity.dataset_id,
            partition_date=ref.identity.partition_date,
            status=ref.status.value,
            config_file=config_file,
            current_stage=ref.stage,
            last_hb=time.time(),
            expires_at=expires_at,
        )

    def to_ref(self) -> TaskRef:
        """Convert back to TaskRef."""
        from apps.ingestion.src.core.models.task.enums import TaskIdentity

        return TaskRef(
            namespace=CACHE_TASK_NAMESPACE,
            status=ExecutionStatus(self.status),
            stage=self.current_stage,
            identity=TaskIdentity(
                job_id=self.job_id,
                dataset_id=self.dataset_id,
                partition_date=self.partition_date,
                run_id=self.run_id,
            ),
        )


# =============================================================================
# TaskRecord - Database Source Record
# =============================================================================


class TaskRecord(msgspec.Struct, kw_only=True):
    """Job record from the database source table."""

    JOB_ID: str
    RUN_ID: str
    DATASET_ID: str
    SCHEDULED_TIMESTAMP_LC: datetime
    JOB_STATUS: str

    PARTITION_DATE: str | None = None
    START_TIMESTAMP_LC: datetime | None = None
    END_TIMESTAMP_LC: datetime | None = None
    LAST_UPDATED_AT_TS_LC: datetime | None = None
    CURRENT_STAGE: str | None = None
    JOB_BITMASK: str | None = None
    IS_SCHEDULED: int = 0
    ERRORS: dict[str, str] | None = None
    RUNTIME_OVERRIDES: dict[str, Any] | None = None
    RETRY_ATTEMPTS: int = 0
    EXPIRATION_THRESHOLD: datetime | None = None
    IS_SNAPSHOT: bool = False

    def __post_init__(self) -> None:
        """Normalize state fields."""
        if not self.JOB_ID or not self.DATASET_ID:
            raise ValueError(f"Invalid TaskRecord: missing IDs for {self}")

        for field in ("CURRENT_STAGE", "JOB_STATUS"):
            val = getattr(self, field, None)
            if isinstance(val, str):
                super().__setattr__(field, val.upper())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create from dictionary."""
        return msgspec.convert(data, cls)

    @property
    def is_triggered(self) -> bool:
        """Whether this job has been triggered (has workspace artifacts)."""
        return self.JOB_STATUS not in (
            ExecutionStatus.PENDING.value,
            ExecutionStatus.UNKNOWN.value,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict with datetime objects converted to strings."""
        result = {}
        for field in self.__struct_fields__:
            value = getattr(self, field)
            if isinstance(value, datetime):
                result[field] = value.isoformat()
            else:
                result[field] = value
        return result


# =============================================================================
# TaskUpdate - State Transition Record
# =============================================================================


class TaskUpdate(msgspec.Struct, kw_only=True):
    """State update to be written to the execution log."""

    JOB_ID: str
    DATASET_ID: str
    PARTITION_DATE: str
    JOB_STATUS: str
    LAST_UPDATED_AT_TS_LC: str

    SCHEDULED_TIMESTAMP_LC: str | None = None
    START_TIMESTAMP_LC: str | None = None
    END_TIMESTAMP_LC: str | None = None
    CURRENT_STAGE: str | None = None
    JOB_BITMASK: str | None = None
    IS_SCHEDULED: int = 1
    ERRORS: dict[str, str] | None = None
    RUNTIME_OVERRIDES: dict[str, Any] | None = None
    RETRY_ATTEMPTS: int = 0
    SOURCE_ROW_COUNT: int | None = None
    FINAL_ROW_COUNT: int | None = None
    FINAL_MANIFEST: str | None = None
    REMARKS: str | None = None

    def __post_init__(self) -> None:
        """Normalize timestamps and stage names."""
        # Normalize stage
        if self.CURRENT_STAGE:
            super().__setattr__("CURRENT_STAGE", self.CURRENT_STAGE.upper())

        # Normalize timestamp fields
        for field in self.__struct_fields__:
            if "TIMESTAMP" in field or field.endswith("_LC"):
                val = getattr(self, field)
                if val:
                    super().__setattr__(field, to_ch_datetime(val))
