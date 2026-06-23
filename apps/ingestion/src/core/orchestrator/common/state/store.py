"""In-memory cache of current state for active runs."""

from threading import RLock
from typing import Any

import msgspec
from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.models.task.enums import TaskRef
from apps.ingestion.src.core.orchestrator.enums import (
    TaskRecord,
    TaskUpdate,
    to_ch_datetime,
)
from apps.ingestion.src.utils.constants import STRIP_TZ_FOR_DB
from libs.utils.dates import current_timestamp
from loguru import logger

LOG = logger


class StateStore:
    """High-performance in-memory cache of current state for active runs.

    The StateStore serves as the 'Hot Registry' for the Orchestrator,
    allowing the 'Tick' loop to evaluate scheduling decisions without
    database round-trips.
    """

    def __init__(self, exec_ctx: ExecutionContext):
        """Initializes the store with thread-safe locking.

        Args:
            exec_ctx: The global execution context.

        Decision: Thread Safety.
        Using an RLock (Reentrant Lock) ensures that the in-memory
        registry remains consistent even when being updated by
        the heartbeat thread while being read by the main dispatch loop.
        """
        self.exec_ctx = exec_ctx
        self.records: dict[str, TaskRecord] = {}
        self._lock = RLock()
        LOG.debug("StateStore initialized")

    @property
    def all(self) -> dict[str, TaskRecord]:
        """Returns a snapshot of all tracked records.

        Returns:
            dict[str, TaskRecord]: A copy of the internal record map.
        """
        with self._lock:
            return self.records.copy()

    def get(self, run_id: str) -> TaskRecord | None:
        """Retrieves a single job record by its run ID.

        Args:
            run_id: The unique identifier for the run.

        Returns:
            TaskRecord | None: The record if found, else None.
        """
        with self._lock:
            record = self.records.get(run_id)
            if record:
                LOG.trace("Retrieved record from cache", run_id=run_id)
            return record

    def add(self, task_ref: TaskRef) -> None:
        """Registers a new task instance in the cache.

        Args:
            task_ref: Reference containing task identity and status.

        Decision: Immutable Identity.
        We derive the initial record from the TaskRef to ensure the
        registry's view of identity (Job/Dataset/Partition) never drifts
        from the physical workspace hierarchy.
        """
        run_id = task_ref.identity.run_id

        with self._lock:
            if run_id in self.records:
                LOG.debug("Record already exists, skipping", run_id=run_id)
                return

            now = current_timestamp(
                timezone=self.exec_ctx.timezone, naive=STRIP_TZ_FOR_DB
            )

            record = TaskRecord(
                JOB_ID=task_ref.identity.job_id,
                DATASET_ID=task_ref.identity.dataset_id,
                PARTITION_DATE=task_ref.identity.partition_date,
                RUN_ID=run_id,
                IS_SCHEDULED=0,
                CURRENT_STAGE=task_ref.stage,
                SCHEDULED_TIMESTAMP_LC=now,
                JOB_STATUS=task_ref.status.value,
            )
            self.records[run_id] = record
            LOG.info(
                "Added record to state cache",
                run_id=run_id,
                job_id=task_ref.identity.job_id,
            )

    def update(self, run_id: str, updates: TaskUpdate | dict[str, Any]) -> bool:
        """Applies updates to an existing record and returns True if state changed.

        Args:
            run_id: Target run ID.
            updates: Partial update object or dictionary.

        Returns:
            bool: True if the update resulted in a data change, False otherwise.

        Decision: Partial Merging.
        By stripping None values from TaskUpdate structs, we allow
        heartbeat updates (like just 'LAST_UPDATED_AT') without
        accidentally overwriting static metadata like the PARTITION_DATE.
        """
        with self._lock:
            current = self.records.get(run_id)
            if not current:
                LOG.warning("Record not found for update", run_id=run_id)
                return False

            if isinstance(updates, TaskUpdate):
                update_dict = {
                    k: v
                    for k, v in msgspec.to_builtins(updates).items()
                    if v is not None
                }
            else:
                update_dict = updates

            merged = {
                **current.to_dict(),
                **update_dict,
                "LAST_UPDATED_AT_TS_LC": to_ch_datetime(
                    current_timestamp(
                        timezone=self.exec_ctx.timezone, naive=STRIP_TZ_FOR_DB
                    )
                ),
            }

            try:
                new_record = msgspec.convert(merged, type=TaskRecord)
                if current == new_record:
                    LOG.trace("No changes detected", run_id=run_id)
                    return False

                self.records[run_id] = new_record
                LOG.trace("Updated record in cache", run_id=run_id)
                return True

            except msgspec.ValidationError:
                LOG.exception("Record validation failed", run_id=run_id)
                return False

    def remove(self, run_id: str) -> None:
        """Permanently removes a record from the in-memory store.

        Args:
            run_id: The ID to evict.
        """
        with self._lock:
            if run_id in self.records:
                self.records.pop(run_id)
                LOG.trace("Removed record from state cache", run_id=run_id)
