"""Trigger management for job scheduling and execution."""

from typing import TYPE_CHECKING, Literal

import msgspec
from apps.ingestion.src.core.contexts.task import TaskContext, load_context
from apps.ingestion.src.core.models.states import ExpiredState
from apps.ingestion.src.core.models.task import ExecutionStatus
from apps.ingestion.src.core.models.task.enums import TaskIdentity
from apps.ingestion.src.utils.constants import CONFIG_FILENAME, STRIP_TZ_FOR_DB
from libs.utils.dates import current_timestamp, parse_timestamp
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.orchestrator.enums import TaskRecord

LOG = logger


class TriggerDecision(msgspec.Struct):
    """Result of trigger evaluation."""

    action: Literal["trigger", "purge", "wait"]
    reason: str
    record: "TaskRecord"
    context: TaskContext | None = None


class TriggerManager:
    """Evaluates job records to determine trigger or purge actions."""

    def __init__(self, exec_ctx):
        self.exec_ctx = exec_ctx

    def evaluate(self, job_records: list["TaskRecord"]) -> list[TriggerDecision]:
        """Iterates through all pending records and calculates actions."""
        # Check shutdown first
        if self.exec_ctx.stop_at_ts is not None:
            LOG.debug("Shutdown active - skipping trigger evaluation")
            return []

        now = current_timestamp(timezone=self.exec_ctx.timezone, naive=STRIP_TZ_FOR_DB)

        decisions = []
        for record in job_records:
            if ExecutionStatus.PENDING.value != record.JOB_STATUS:
                continue

            # Check purge conditions (expiry policy)
            if self._should_purge(record):
                context = self._try_load_context(record)
                decisions.append(
                    TriggerDecision(
                        action="purge",
                        reason="PURGE_POLICY",
                        record=record,
                        context=context,
                    )
                )
                continue

            # Check trigger conditions
            if self._should_trigger(record, now):
                context = self._try_load_context(record)
                decisions.append(
                    TriggerDecision(
                        action="trigger",
                        reason="READY:DISPATCH",
                        record=record,
                        context=context,
                    )
                )

        return decisions

    def _should_purge(self, record: "TaskRecord") -> bool:
        """Check if job should be purged based on temporal expiry policies."""
        return ExpiredState.matches(task=None, record=record, exec_ctx=self.exec_ctx)

    def _should_trigger(self, record: "TaskRecord", now) -> bool:
        """Check if job should be triggered based on its type."""
        # 1. Check scheduled time
        scheduled = parse_timestamp(
            record.SCHEDULED_TIMESTAMP_LC, naive=STRIP_TZ_FOR_DB
        )
        return now >= scheduled

    def _try_load_context(self, record: "TaskRecord") -> TaskContext | None:
        """Load task context from disk.

        if config file, task already triggered before.
        """
        identity = TaskIdentity(
            job_id=record.JOB_ID,
            dataset_id=record.DATASET_ID,
            partition_date=str(record.PARTITION_DATE),
            run_id=record.RUN_ID,
        )

        run_path = self.exec_ctx.get_run_path(identity)
        pending_path = (
            self.exec_ctx.active_path
            / f"{identity.task_key}:{record.RUN_ID}_{CONFIG_FILENAME}"
        )
        if run_path.exists():
            return load_context(run_path)

        if pending_path.exists():
            with pending_path.open("rb") as f:
                return msgspec.json.decode(f.read(), type=TaskContext)

        # Log ghost tasks without caching complexity
        if record.RUN_ID and self.exec_ctx.get_run_path(identity).exists():
            LOG.warning(f"Ghost task detected: {record.RUN_ID}")

        return None
