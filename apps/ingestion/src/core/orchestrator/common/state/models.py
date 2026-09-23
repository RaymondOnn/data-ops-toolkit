from datetime import datetime
from typing import Any

import msgspec
from libs.utils.dates import parse_timestamp
from loguru import logger

from src.core.models.task.status import ExecutionStatus
from src.utils.constants import STRIP_TZ_FOR_DB

LOG = logger
# =============================================================================
# Helper Functions
# =============================================================================


def to_ch_datetime(ts: Any) -> str | None:
    """
    Convert timestamp to ClickHouse DateTime64(3) format.

    Returns 'YYYY-MM-DD HH:MM:SS.SSS' without timezone.
    """
    from libs.utils.dates import parse_timestamp

    from src.utils.constants import STRIP_TZ_FOR_DB

    if not ts:
        return None

    try:
        dt = parse_timestamp(ts, naive=STRIP_TZ_FOR_DB)
        return dt.format("YYYY-MM-DD HH:mm:ss.SSS")
    except Exception:
        # Fallback: brute-force strip
        return str(ts).replace("T", " ").split("+")[0].split("Z")[0]


# =============================================================================
# TaskRecord - Database Source Record
# =============================================================================


class TaskRecord(msgspec.Struct, kw_only=True):
    """Job record from the database source table."""

    JOB_ID: str
    RUN_ID: str
    DATASET_ID: str
    SCHEDULED_TIMESTAMP_LC: datetime

    JOB_STATUS: str | None = None
    PARTITION_DATE: str | None = None
    START_TIMESTAMP_LC: datetime | None = None
    END_TIMESTAMP_LC: datetime | None = None
    LAST_UPDATED_AT_TS_LC: datetime | None = None
    CURRENT_STEP: str | None = None
    JOB_BITMASK: str | None = None
    IS_SCHEDULED: int = 0
    ERRORS: dict[str, str] | None = None
    RUNTIME_OVERRIDES: dict[str, Any] | None = None
    RETRY_ATTEMPTS: int = 0
    EXPIRATION_THRESHOLD: datetime | None = None
    IS_SNAPSHOT: bool = False

    _ALIAS_MAP = {
        "status": "JOB_STATUS",
        "scheduled_at": "SCHEDULED_TIMESTAMP_LC",
    }

    def __post_init__(self) -> None:
        """Normalize state fields."""
        if not self.JOB_ID or not self.DATASET_ID:
            raise ValueError(f"Invalid TaskRecord: missing IDs for {self}")

        for field in ("CURRENT_STEP", "JOB_STATUS"):
            val = getattr(self, field, None)
            if isinstance(val, str):
                super().__setattr__(field, val.upper())

    def __getattr__(self, name: str) -> Any:
        # Check custom mappings first, then fallback to uppercase attribute name
        target_field = self._ALIAS_MAP.get(name, name.upper())
        if target_field in self.__struct_fields__:
            return getattr(self, target_field)
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'"
        )

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

    def apply_update(self, update: "TaskUpdate") -> bool:
        """Applies a TaskUpdate delta to this TaskRecord in-place."""
        # Guard: Do not revert terminal states
        if (
            self.JOB_STATUS
            and ExecutionStatus(self.JOB_STATUS).is_terminal
            and not ExecutionStatus(update.JOB_STATUS).is_terminal
        ):
            LOG.warning(
                f"Blocked status reversal for RUN_ID={self.RUN_ID}: "
                f"{self.JOB_STATUS} -> {update.JOB_STATUS}"
            )
            return False

        # 1. Update status
        self.JOB_STATUS = update.JOB_STATUS

        # 2. Parse string timestamps into pendulum.DateTime/datetime using project utils
        if update.LAST_UPDATED_AT_TS_LC:
            self.LAST_UPDATED_AT_TS_LC = parse_timestamp(
                update.LAST_UPDATED_AT_TS_LC, naive=STRIP_TZ_FOR_DB
            )

        if update.START_TIMESTAMP_LC:
            self.START_TIMESTAMP_LC = parse_timestamp(
                update.START_TIMESTAMP_LC, naive=STRIP_TZ_FOR_DB
            )

        if update.END_TIMESTAMP_LC:
            self.END_TIMESTAMP_LC = parse_timestamp(
                update.END_TIMESTAMP_LC, naive=STRIP_TZ_FOR_DB
            )

        # 3. Update optional step and execution state
        if update.CURRENT_STEP is not None:
            self.CURRENT_STEP = update.CURRENT_STEP

        if update.ERRORS is not None:
            self.ERRORS = update.ERRORS

        if update.RETRY_ATTEMPTS:
            self.RETRY_ATTEMPTS = update.RETRY_ATTEMPTS

        return True


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
    CURRENT_STEP: str | None = None
    PROGRESS: str | None = None
    IS_SCHEDULED: int = 1
    ERRORS: dict[str, str] | None = None
    RUNTIME_OVERRIDES: dict[str, Any] | None = None
    RETRY_ATTEMPTS: int = 0
    # SOURCE_ROW_COUNT: str | None = None
    FINAL_ROW_COUNT: int | None = None
    FINAL_MANIFEST: str | None = None
    REMARKS: str | None = None

    def __post_init__(self) -> None:
        """Normalize timestamps and stage names."""
        # Normalize stage
        if self.CURRENT_STEP:
            super().__setattr__("CURRENT_STEP", self.CURRENT_STEP.upper())

        # Normalize timestamp fields
        for field in self.__struct_fields__:
            if "TIMESTAMP" in field or field.endswith("_LC"):
                val = getattr(self, field)
                if val:
                    super().__setattr__(field, to_ch_datetime(val))

    # @classmethod
    # def from_record(cls, record: "TaskRecord") -> "TaskUpdate":
    #     """Factory method to cast a full TaskRecord snapshot down to an event TaskUpdate."""
    #     # Convert the TaskRecord to a plain dictionary of primitive values
    #     record_dict = msgspec.to_builtins(record)

    #     # Pull out only the fields that are valid for TaskUpdate definitions
    #     update_fields = {
    #         k: v for k, v in record_dict.items() if k in cls.__struct_fields__
    #     }

    #     return msgspec.convert(update_fields, type=cls)
