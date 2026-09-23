"""Central hub coordinating all state operations."""

from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec
from loguru import logger

from src.core.contexts import ExecutionContext, TaskContext
from src.core.models.task import TaskManifestFile
from src.utils.common import find_path

from .models import TaskUpdate
from .sink import StateSink
from .source import StateSource
from .store import StateStore

if TYPE_CHECKING:
    from src.services.repo.metadata import MetadataRepository

LOG = logger


# TODO:
class StateHub:
    """Central hub coordinating all state operations for the orchestrator.

    The Hub acts as a facade, routing state updates between the high-speed
    in-memory cache (StateStore), the disk-persistent stream (StateStream),
    and the physical manifest files (StateSource).

    Notes:
    - By wrapping the three pillars of state management into a single Hub, we
    simplify the Orchestrator's API. The rest of the system only needs to
    know about the Hub, which ensures that an update to the cache is
    correctly mirrored to the log stream and the database.
    """

    def __init__(self, meta_repo: "MetadataRepository", exec_ctx: "ExecutionContext"):
        """Initializes the Hub and its underlying state components.

        Args:
            db_config: Configuration for the ClickHouse telemetry sink.
            exec_ctx: The global execution context for workspace pathing.

        """
        self.exec_ctx = exec_ctx
        self.workspace_dir = exec_ctx.state_path
        self.workspace_dir.mkdir(parents=True, exist_ok=True)

        self.store = StateStore(exec_ctx)
        self.sink = StateSink(meta_repo, self.workspace_dir)
        self.source = StateSource(self.store, exec_ctx)

        LOG.trace("StateHub initialized", workspace=str(self.workspace_dir))

    def update_task(self, run_id: str, updates: TaskUpdate | dict[str, Any]) -> None:
        """Applies partial updates to a task and flushes to the sink.

        Args:
            run_id: Unique identifier for the run.
            updates: A TaskUpdate struct or dictionary of changes.

        Notes:
            The stream only receives an update if the store detects an
            actual value change. This prevents redundant I/O for
            idempotent heartbeats.
        """
        LOG.trace("Updating task state", run_id=run_id, updates=str(updates)[:200])

        try:
            # Single point of structural validation!
            task_update = msgspec.convert(updates, type=TaskUpdate)

            # Route updates
            if self.store.update(run_id, task_update):
                self.sink.append(task_update)
        except Exception:
            LOG.exception("Update failed", run_id=run_id)
            raise

    def remove_task(self, run_id: str) -> None:
        """Evicts a task from the in-memory registry.

        Args:
            run_id: The ID to remove.
        """
        self.store.remove(run_id)

    def sync_manifest(
        self,
        folder_path: Path | str,
        deep_sync: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Synchronizes a physical manifest file back into the state engine.

        The manifest file is used as canonical log for the task.
        Hence, saving ir to database for audit purposes

        Args:
            folder_path: Path to the task's workspace folder.
            deep_sync: If True, includes the full manifest JSON in the update.
        """
        metadata = metadata or {}

        resolved_path = Path(folder_path)
        if not resolved_path.exists():
            resolved = find_path(self.exec_ctx.workspace_dir, str(folder_path))
            if not resolved:
                LOG.warning(f"Could not resolve path for manifest sync: {folder_path}")
                return
            resolved_path = resolved

        task_manifest = TaskManifestFile.load(folder_path=resolved_path)
        task_context = TaskContext.from_path(folder_path=resolved_path)

        task_update = self.source.parse_update(
            task_manifest, task_context, deep_sync, metadata
        )

        # Route updates
        if self.store.update(task_manifest.run_id, task_update):
            self.sink.append(task_update)

    def find_task_path(self, identifier: str) -> Path | None:
        """Resolves the physical workspace path for a given task ID.

        Args:
            identifier: The run_id or task identifier.

        Returns:
            Path | None: The absolute path if found.
        """
        path = find_path(self.exec_ctx.workspace_dir, identifier)
        if path and path.exists():
            LOG.trace("Found task path", identifier=identifier, path=str(path))
        else:
            LOG.trace("Task path not found", identifier=identifier)
        return path if path and path.exists() else None

    def flush(self) -> None:
        """Forces a flush of all buffered sink data to ClickHouse."""
        LOG.info("Flushing state to database")
        self.sink.flush_to_db()

    def close(self) -> None:
        """Gracefully shuts down the Hub and closes active streams."""
        LOG.trace("Closing StateHub")
        self.sink.flush_to_db()
