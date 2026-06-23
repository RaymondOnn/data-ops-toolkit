from enum import StrEnum

import msgspec
from apps.ingestion.src.core.models.task.status import ExecutionStatus


class TaskSignal(StrEnum):
    SYNC = "sync"
    DONE = "done"
    FAIL = "fail"
    RETRY = "retry"
    EXPIRED = "expired"


# Centralized mapping of logical signals to physical file extensions
SUPPORTED_SIGNAL_EXTENSIONS = {f".{s.value}" for s in TaskSignal}


class TaskIdentity(msgspec.Struct, frozen=True):
    """Immutable physical identity of a task run."""

    job_id: str
    dataset_id: str
    partition_date: str
    run_id: str

    @classmethod
    def from_signal_stem(cls, stem: str) -> "TaskIdentity":
        """Parse signal filename stem into identity."""
        parts = stem.split(":")
        if len(parts) != 4:
            raise ValueError(f"Invalid signal stem: {stem}")
        return cls(*parts)

    @property
    def task_key(self) -> str:
        """Logical task key (JOB:DATASET:PARTITION)."""
        return f"{self.job_id}:{self.dataset_id}:{self.partition_date}"


class TaskRef(msgspec.Struct, frozen=True):
    """Routing handle combining identity with current execution state."""

    identity: TaskIdentity
    status: ExecutionStatus
    stage: str
    namespace: str = "task"

    @classmethod
    def from_key(cls, key: str) -> "TaskRef":
        """Parse colon-delimited string into TaskRef."""
        parts = key.split(":")
        if len(parts) != 7:
            raise ValueError(f"Invalid TaskRef format: {key}")

        return cls(
            namespace=parts[0],
            status=ExecutionStatus(parts[1]),
            stage=parts[2],
            identity=TaskIdentity(*parts[3:]),
        )

    @property
    def task_key(self) -> str:
        return self.identity.task_key

    @property
    def run_id(self) -> str:
        return self.identity.run_id

    def build(self, status: str | None = None, stage: str | None = None) -> str:
        """Build cache key string with optional overrides."""
        return (
            f"{self.namespace}:"
            f"{status or self.status.value}:"
            f"{stage or self.stage}:"
            f"{self.identity.job_id}:{self.identity.dataset_id}:{self.identity.partition_date}:"
            f"{self.identity.run_id}"
        )

    def with_updates(
        self, status: ExecutionStatus | None = None, stage: str | None = None
    ) -> "TaskRef":
        """Create new TaskRef with updated status/stage."""
        return msgspec.structs.replace(
            self,
            status=status if status is not None else self.status,
            stage=stage if stage is not None else self.stage,
        )
