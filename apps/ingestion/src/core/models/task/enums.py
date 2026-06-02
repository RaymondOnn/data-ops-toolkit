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
    """
    The immutable physical identity of a task run.
    Used for path construction, signal naming, and unique identification.
    """

    job_id: str
    dataset_id: str
    partition_date: str
    run_id: str

    @classmethod
    def from_signal_stem(cls, stem: str) -> "TaskIdentity":
        """Parses a signal file stem (job:dataset:partition:run_id) into Identity."""
        parts = stem.split(":")
        if len(parts) != 4:
            raise ValueError(f"Invalid signal stem: {stem}")
        return cls(*parts)

    @property
    def identifier(self) -> str:
        """The logical task identifier (JOB:DATASET:PARTITION)."""
        return f"{self.job_id}:{self.dataset_id}:{self.partition_date}"

    @property
    def composite_key(self) -> str:
        """The registry lookup key (JOB:DATASET)."""
        return f"{self.job_id}:{self.dataset_id}"


class TaskRef(msgspec.Struct, frozen=True):
    """
    A routing handle that combines a TaskIdentity with its current Execution State.
    Used primarily for Cache Key generation and Orchestrator routing.
    """

    identity: TaskIdentity
    status: ExecutionStatus
    stage: str
    namespace: str = "task"

    @classmethod
    def from_str(cls, key: str) -> "TaskRef":
        """Factory to parse a colon-delimited string into a TaskRef Struct."""
        parts = key.split(":")
        if len(parts) != 7:
            raise ValueError(f"Invalid TaskRef format: {key}")

        # Format: namespace:status:stage:job:ds:date:run
        return cls(
            namespace=parts[0],
            status=ExecutionStatus(parts[1]),
            stage=parts[2],
            identity=TaskIdentity(*parts[3:]),
        )

    @property
    def identifier(self) -> str:
        return self.identity.identifier

    @property
    def run_id(self) -> str:
        return self.identity.run_id

    def build(self, status: str | None = None, stage: str | None = None) -> str:
        """Rebuilds a cache key with optional state changes."""
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
        """Returns a new TaskRef with updated status or stage, bypassing string parsing."""
        return msgspec.structs.replace(
            self,
            status=status if status else self.status,
            stage=stage if stage else self.stage,
        )
