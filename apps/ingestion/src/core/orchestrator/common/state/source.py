"""Loads state updates from task manifests on disk."""

from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec
from apps.ingestion.src.core.contexts import ExecutionContext, TaskContext
from apps.ingestion.src.core.models.stages.enums import ALL_STAGES
from apps.ingestion.src.core.models.task import ExecutionStatus, TaskManifest
from apps.ingestion.src.core.orchestrator.enums import TaskUpdate, to_ch_datetime
from apps.ingestion.src.utils.constants import (
    CACHE_TASK_NAMESPACE,
    CONFIG_FILENAME,
    MANIFEST_FILENAME,
)
from libs.utils.dates import current_timestamp
from loguru import logger

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
        LOG.debug("StateSource initialized")

    def sync_folder(self, folder_path: Path, deep_sync: bool = False) -> None:
        """Synchronizes a physical task folder with the orchestrator state.

        This reads the manifest and config files from disk and notifies
        the store and sink of the task's latest status.

        Args:
            folder_path: The physical directory for the run.
            deep_sync: If True, serializes the entire manifest into the
                telemetry database for deep audit.

        Decision: Manifest Synchronization.
        By rehydrating state from disk, we enable 'Cold Start'
        resumption. If the orchestrator daemon crashes, it can
        reconstruct its internal cache by scanning the 'active/'
        folder and invoking this method.
        """
        manifest_file = folder_path / MANIFEST_FILENAME
        config_file = folder_path / CONFIG_FILENAME

        if not manifest_file.exists():
            LOG.warning("No manifest found for sync", path=str(folder_path))
            return

        LOG.debug("Syncing manifest to state", path=str(folder_path), deep=deep_sync)

        try:
            manifest = msgspec.json.decode(
                manifest_file.read_bytes(), type=TaskManifest
            )
            LOG.trace(
                "Loaded manifest", run_id=manifest.run_id, status=manifest.status.value
            )

            context = self._load_context(manifest, config_file)
            self._parse_update(manifest, context, deep_sync)
            LOG.debug("Successfully synced manifest", run_id=manifest.run_id)

        except msgspec.DecodeError as e:
            LOG.error(
                "Failed to decode manifest", path=str(manifest_file), error=str(e)
            )
        except Exception:
            LOG.exception("Sync failed", path=str(folder_path))

    def _load_context(
        self, manifest: TaskManifest, config_file: Path
    ) -> TaskContext | None:
        """Attempts to find the TaskContext required for state alignment.

        Decision: Context Discovery.
        We prioritize the local config file found inside the task
        folder. If missing (e.g., during surgical recovery), we
        attempt to build a placeholder from the current registry to
        ensure the state update can still be processed.
        """
        if config_file.exists():
            try:
                with config_file.open("rb") as f:
                    return msgspec.json.decode(f.read(), type=TaskContext)
            except Exception as e:
                LOG.debug(
                    "Failed to load config file", path=str(config_file), error=str(e)
                )
        return None

    def _parse_update(
        self,
        manifest: TaskManifest,
        context: TaskContext | None,
        deep_sync: bool,
        metadata: dict[str, Any] | None = None,
    ) -> None:
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
        # Resolve status
        if metadata and "status" in metadata:
            status = metadata["status"]
            status = status.value if isinstance(status, ExecutionStatus) else status
        else:
            status = manifest.status.value

        progress = self._format_bitmask(manifest.bitmask, status)
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
            context.overrides if context else getattr(record, "RUNTIME_OVERRIDES", None)
        )

        LOG.debug(
            "Emitting state update",
            run_id=manifest.run_id,
            status=status,
            progress=progress,
            remarks=remarks,
        )

        update = TaskUpdate(
            JOB_ID=manifest.job_id,
            DATASET_ID=manifest.dataset_id,
            PARTITION_DATE=partition_date,
            SCHEDULED_TIMESTAMP_LC=to_ch_datetime(record.SCHEDULED_TIMESTAMP_LC),
            IS_SCHEDULED=record.IS_SCHEDULED,
            JOB_STATUS=status,
            CURRENT_STAGE=manifest.current_stage,
            JOB_BITMASK=progress,
            RETRY_ATTEMPTS=manifest.retry_count,
            LAST_UPDATED_AT_TS_LC=current_timestamp().isoformat(sep=" "),
            START_TIMESTAMP_LC=getattr(manifest.start, "start_time", None),
            END_TIMESTAMP_LC=getattr(manifest.archive, "end_time", None),
            SOURCE_ROW_COUNT=getattr(manifest.extract, "source_count", None),
            FINAL_ROW_COUNT=getattr(manifest.publish, "final_count", None),
            RUNTIME_OVERRIDES=overrides,
            FINAL_MANIFEST=(
                msgspec.json.encode(manifest).decode() if deep_sync else None
            ),
            ERRORS=msgspec.to_builtins(manifest.error) if manifest.error else None,
            REMARKS=remarks,
        )

        if self.store.update(manifest.run_id, update):
            LOG.debug(
                "Updated state from manifest",
                run_id=manifest.run_id,
                status=status,
                remarks=remarks,
            )
        else:
            LOG.debug("No changes from manifest", run_id=manifest.run_id)

    def _build_remarks(
        self, manifest: TaskManifest, status: str, metadata: dict | None
    ) -> str | None:
        """Build remarks with special handling for RETRY and BLOCKED statuses."""
        remarks = (metadata or {}).get("remarks")
        if remarks:
            return remarks

        if status == ExecutionStatus.RETRY.value:
            ts = to_ch_datetime(current_timestamp())
            return f"Transient error, retrying (attempt {manifest.retry_count}) at {ts}"

        if status == ExecutionStatus.BLOCKED.value:
            # Try to get blocking service name
            if manifest.error and manifest.error.message:
                return manifest.error.message

            from libs.cache.factory import get_cache

            cache = get_cache(self.exec_ctx.workspace_dir, self.exec_ctx.cache_config)
            pattern = f"{CACHE_TASK_NAMESPACE}:*:*:*:*:*:{manifest.run_id}"

            for key in list(cache.iterkeys(pattern=pattern)):
                task_meta = cache.get(key)
                if task_meta and getattr(task_meta, "blocked_by", None):
                    return f"Task blocked by service '{task_meta.blocked_by}'"

            return "Task blocked by unknown service"

        return None

    @staticmethod
    def _format_bitmask(bitmask: int, status: str) -> str:
        """Translates a numeric stage bitmask into a human-readable progress bar.

        Args:
            bitmask: The current completion bitmask.
            status: The execution status (for handling failure markers).

        Returns:
            str: A formatted string like "EXT+ | TRN- | WRI_".

        Decision: Progress Visualization.
        Providing a 'Timeline' string in the database log allows SREs
        to instantly see where a task stalled without needing to
        unzip and parse terminal logs.
        """
        stages = ALL_STAGES
        failed = status.upper() == ExecutionStatus.FAILED.value
        failed_seen = False
        parts = []

        for stage in stages:
            if bitmask & stage.bitmask:
                parts.append(f"{stage.token}+")
            elif failed and not failed_seen:
                parts.append(f"{stage.token}-")
                failed_seen = True
            else:
                parts.append(f"{stage.token}_")

        return " | ".join(parts)
