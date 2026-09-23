import time
from pathlib import Path
from typing import Any, Self

import msgspec

from src.core.models.task.enums import TaskRef
from src.core.models.task.status import ExecutionStatus
from src.core.orchestrator.common.task.timeout import TimeoutState
from src.core.stages.types import Stage
from src.utils.constants import CACHE_TASK_NAMESPACE

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
    current_step_id: str
    status: str = "WAITING"
    last_hb: float = msgspec.field(default_factory=time.time)
    next_attempt_ts: str | None = None
    retry_count: int = 0
    rollback_history: dict[str, str] = msgspec.field(default_factory=dict)
    rollback_stack: list[str] = msgspec.field(default_factory=list)
    expires_at: float | None = None
    blocked_by: str | None = None
    timeout_state: TimeoutState | None = None
    scheduled_at: float = msgspec.field(default_factory=time.time)

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
            current_step_id=ref.step_id,
            last_hb=time.time(),
            expires_at=expires_at,
        )

    def to_ref(self) -> TaskRef:
        """Convert back to TaskRef."""
        from src.core.models.task.enums import TaskIdentity

        return TaskRef(
            namespace=CACHE_TASK_NAMESPACE,
            status=ExecutionStatus(self.status),
            step_id=self.current_step_id,
            identity=TaskIdentity(
                job_id=self.job_id,
                dataset_id=self.dataset_id,
                partition_date=self.partition_date,
                run_id=self.run_id,
            ),
        )

    def generate_cache_key(self, status_override: ExecutionStatus | None = None) -> str:
        """Centralizes key construction logic so it never leaks into business loops."""
        status_val = status_override.value if status_override else self.status
        return self.to_ref().build(status=status_val, step_id=self.current_step_id)

    @classmethod
    def from_raw_cache(cls, raw_data: Any) -> Self:
        """Safely decompresses or deserializes raw cache records."""
        if isinstance(raw_data, dict):
            return msgspec.convert(raw_data, type=cls)
        return msgspec.json.decode(raw_data, type=cls)

    def _get_task_context(self):
        from src.core.contexts.task import load_context

        return load_context(Path(self.config_file).parent)

    @property
    def current_step(self):
        ctx = self._get_task_context()
        return ctx.get_step(self.current_step_id)

    @property
    def current_stage(self) -> str:
        """Helper to resolve stage on-the-fly."""
        if self.current_step_id == "start":
            return Stage.START.value
        if context := self._get_task_context():
            return context.resolve_stage(self.current_step_id)
        # Fallback if context isn't loaded yet
        return Stage.START.value
