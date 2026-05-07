from enum import StrEnum

import msgspec
from apps.ingestion.src.utils.constants import CACHE_TASK_NAMESPACE


class TaskSignal(StrEnum):
    SYNC = "sync"
    DONE = "done"
    FAIL = "fail"
    RETRY = "retry"
    EXPIRED = "expired"


class TaskRef(msgspec.Struct, frozen=True):
    """
    A lightweight handle representing a task's identity and current routing state.
    Acts as the single source of truth for identifying a task across the orchestrator,
    cache, and filesystem.
    """

    namespace: str
    status: str
    stage: str
    job_id: str
    dataset_id: str
    partition_date: str
    run_id: str

    @classmethod
    def from_str(cls, key: str) -> "TaskRef":
        """Factory to parse a colon-delimited string into a TaskRef Struct."""
        parts = key.split(":")
        if len(parts) != 7:
            raise ValueError(f"Invalid TaskRef format: {key}")
        return cls(*parts)

    @classmethod
    def from_signal_stem(cls, stem: str) -> "TaskRef":
        """Parses a signal file stem (job:dataset:partition:run_id) into a Ref."""
        parts = stem.split(":")
        if len(parts) != 4:
            raise ValueError(f"Invalid signal stem: {stem}")
        return cls(
            namespace="task",
            status="UNKNOWN",
            stage="UNKNOWN",
            job_id=parts[0],
            dataset_id=parts[1],
            partition_date=parts[2],
            run_id=parts[3],
        )

    @property
    def identifier(self) -> str:
        """The logical task identifier (JOB:DATASET:PARTITION)."""
        return f"{self.job_id}:{self.dataset_id}:{self.partition_date}"

    @property
    def composite_key(self) -> str:
        """The registry lookup key (JOB:DATASET)."""
        return f"{self.job_id}:{self.dataset_id}"

    def build(self, status: str | None = None, stage: str | None = None) -> str:
        """Rebuilds a cache key with optional state changes."""
        return (
            f"{CACHE_TASK_NAMESPACE}:"
            f"{status or self.status}:"
            f"{stage or self.stage}:"
            f"{self.identifier}:"
            f"{self.run_id}"
        )

    def with_updates(
        self, status: str | None = None, stage: str | None = None
    ) -> "TaskRef":
        """Returns a new TaskRef with updated status or stage, bypassing string parsing."""
        return msgspec.structs.replace(
            self,
            status=status if status else self.status,
            stage=stage if stage else self.stage,
        )
