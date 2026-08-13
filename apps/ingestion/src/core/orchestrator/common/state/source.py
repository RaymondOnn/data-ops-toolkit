"""Loads state updates from task manifests on disk."""

from typing import TYPE_CHECKING, Any

import msgspec
from libs.utils.dates import current_timestamp
from loguru import logger

from src.core.contexts import ExecutionContext, TaskContext
from src.core.models.task import ExecutionStatus, TaskManifest
from src.core.orchestrator.enums import TaskUpdate, to_ch_datetime

if TYPE_CHECKING:
    from .store import StateStore

LOG = logger


class StateSource:
    """Loads state updates from task manifests on disk."""

    def __init__(
        self,
        store: "StateStore",
        exec_ctx: "ExecutionContext",
    ):
        self.store = store
        self.exec_ctx = exec_ctx
        LOG.trace("StateSource initialized")

    def parse_update(
        self,
        manifest: TaskManifest,
        context: TaskContext,
        deep_sync: bool,
        metadata: dict[str, Any] | None = None,
    ) -> TaskUpdate:
        """Translates a disk manifest and context into a TaskUpdate telemetry event.

        Decision: Attribute Extraction.
        The parser extracts business metrics (row counts, duration,
        retry attempts) directly from the stage-specific manifest
        objects. This ensures the database log remains the
        authoritative source for data-lineage audits.

        Args:
            manifest: The task manifest to parse.
            context: The task context.
            deep_sync: Whether to include full manifest in update.
            metadata: Optional override metadata (status, remarks, etc.).
        """
        try:
            # Resolve status
            if metadata and "status" in metadata:
                status = metadata["status"]
                status = status.value if isinstance(status, ExecutionStatus) else status
            else:
                status = manifest.status.value

            progress, current_step = self._calculate_step_progress(
                manifest, context, status
            )
            start_time, end_time = self._extract_timestamps(manifest)
            source_row_count, final_row_count = self._extract_row_counts(manifest)
            remarks = self._build_remarks(manifest, status, metadata)

            # Get record and resolve context info
            record = self.store.get(manifest.run_id)
            if not record:
                raise ValueError(
                    f"Unable to find state record for run_id={manifest.run_id}"
                )

            partition_date = (
                context.partition_date
                if context
                else getattr(record, "PARTITION_DATE", "N/A")
            )
            overrides = (
                context.overrides
                if context
                else getattr(record, "RUNTIME_OVERRIDES", None)
            )

            LOG.trace(
                "Emitting state update",
                run_id=manifest.run_id,
                status=status,
                progress=progress,
                remarks=remarks,
            )

            return TaskUpdate(
                JOB_ID=manifest.job_id,
                DATASET_ID=manifest.dataset_id,
                PARTITION_DATE=partition_date,
                SCHEDULED_TIMESTAMP_LC=to_ch_datetime(record.SCHEDULED_TIMESTAMP_LC),
                IS_SCHEDULED=record.IS_SCHEDULED,
                JOB_STATUS=status,
                CURRENT_STEP=current_step,
                PROGRESS=progress,
                RETRY_ATTEMPTS=manifest.retry_count,
                LAST_UPDATED_AT_TS_LC=current_timestamp().isoformat(sep=" "),
                START_TIMESTAMP_LC=start_time,
                END_TIMESTAMP_LC=end_time,
                SOURCE_ROW_COUNT=source_row_count,
                FINAL_ROW_COUNT=final_row_count,
                RUNTIME_OVERRIDES=overrides,
                FINAL_MANIFEST=(
                    msgspec.json.encode(manifest).decode() if deep_sync else None
                ),
                ERRORS=msgspec.to_builtins(manifest.error) if manifest.error else None,
                REMARKS=remarks,
            )

            # if self.store.update(manifest.run_id, update):
            #     LOG.trace(
            #         "Updated state from manifest",
            #         run_id=manifest.run_id,
            #         status=status,
            #         remarks=remarks,
            #     )
            # else:
            #     LOG.trace("No changes from manifest", run_id=manifest.run_id)

        except Exception:
            LOG.exception("Failed parsing updates from disk")
            raise

    @staticmethod
    def _build_remarks(
        manifest: TaskManifest, status: str, metadata: dict | None
    ) -> str | None:
        """Build remarks with special handling for RETRY and BLOCKED statuses.

        Args:
            manifest: The task manifest.
            status: The current status as a string.
            metadata: Optional metadata containing remarks, blocked_by, etc.

        Returns:
            str | None: Formatted remarks string.
        """
        remarks = None
        # Priority 1: Explicit remarks override everything
        explicit_remarks = (metadata or {}).get("remarks")
        if explicit_remarks:
            remarks = explicit_remarks

        # Priority 2: Status-specific handling
        if status == ExecutionStatus.RETRY.value:
            ts = to_ch_datetime(current_timestamp())
            remarks = (
                f"Transient error, retrying (attempt {manifest.retry_count}) at {ts}"
            )

        if status == ExecutionStatus.BLOCKED.value:
            blocked_by = (metadata or {}).get("blocked_by")
            if blocked_by:
                remarks = f"Task blocked by '{blocked_by}'"
            if manifest.error and manifest.error.message:
                remarks = manifest.error.message
            if not remarks:
                remarks = "Task blocked by unknown service"

        if status == ExecutionStatus.FAILED.value and manifest.error:
            remarks = manifest.error.message

        return remarks

    @staticmethod
    def _calculate_step_progress(
        manifest: TaskManifest, context: TaskContext | None, status: str
    ) -> tuple[str | None, str | None]:
        """Calculates 'Step X of Y' progress taking into account from_step and to_step boundaries."""
        current_step_id = manifest.current_step_id or getattr(
            manifest, "current_step_id", None
        )

        if not context or not context.steps:
            return None, current_step_id

        # 1. get_step_ids() directly gives us the bounded active step sequence
        active_step_ids = context.get_step_ids()
        if not active_step_ids:
            return None, current_step_id

        total_active_count = len(active_step_ids)

        # 2. Match completed step IDs against active step scope
        completed_ids = set(manifest.completed_step_ids)
        completed_count = sum(1 for sid in active_step_ids if sid in completed_ids)

        # 3. Calculate ordinal step position
        if status == ExecutionStatus.SUCCESS.value:
            current_index = total_active_count
        elif current_step_id in active_step_ids:
            current_index = active_step_ids.index(current_step_id) + 1
        else:
            current_index = min(completed_count + 1, total_active_count)

        progress_str = f"Step {current_index} of {total_active_count}"

        # 4. Concise step indicator with boundary context if partial execution
        formatted_step = current_step_id
        if context.from_step != "start" or context.to_step:
            formatted_step = (
                f"{current_step_id} [{context.from_step}..{context.to_step or 'end'}]"
            )

        return progress_str, formatted_step

    @staticmethod
    def _extract_timestamps(manifest: TaskManifest) -> tuple[str | None, str | None]:
        """Extracts task overall start and end times from executed payloads."""
        if not manifest.payloads:
            return None, None

        # Start time is from the first payload
        start_time = getattr(manifest.payloads[0], "start_time", None)

        # End time is taken from the latest payload only when completed/failed
        end_time = None
        if manifest.status in (
            ExecutionStatus.SUCCESS,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
        ):
            end_time = getattr(manifest.payloads[-1], "end_time", None)

        return start_time, end_time

    @staticmethod
    def _extract_row_counts(manifest: TaskManifest) -> tuple[str | None, int | None]:
        """Extracts per-source row counts as a JSON string and identifies final output count."""
        from src.core.stages.extract.enums import ExtractPayload
        from src.core.stages.publish.enums import PublishPayload
        from src.core.stages.write.enums import WritePayload

        source_counts: dict[str, int] = {}
        final_count = None

        for payload in manifest.payloads:
            # 1. Map each extract step/resource to its specific row count
            if isinstance(payload, ExtractPayload):
                # Prefers resource/source identifier, falls back to step_id
                source_key = payload.resource or payload.step_id or "unknown_source"
                source_counts[source_key] = payload.source_count

            # 2. Get the final output count from Publish or Write payload
            if isinstance(payload, PublishPayload):
                final_count = payload.final_count
            elif isinstance(payload, WritePayload) and final_count is None:
                final_count = payload.write_count

        # Convert dictionary to JSON string if any sources were extracted
        source_counts_json = (
            msgspec.json.encode(source_counts).decode("utf-8")
            if source_counts
            else None
        )

        return source_counts_json, final_count
