"""Central hub coordinating all state operations."""

from pathlib import Path
from typing import Any

from apps.ingestion.src.core.contexts.execution import ExecutionContext
from apps.ingestion.src.core.models.task.enums import TaskRef
from apps.ingestion.src.core.orchestrator.enums import TaskUpdate
from apps.ingestion.src.utils.common import find_path
from loguru import logger

from .sink import StateSink
from .source import StateSource
from .store import StateStore

LOG = logger


# TODO:
class StateHub:
    """Central hub coordinating all state operations for the orchestrator.

    The Hub acts as a facade, routing state updates between the high-speed
    in-memory cache (StateStore), the disk-persistent stream (StateStream),
    and the physical manifest files (StateSource).

    Decision: Centralized Coordination.
    By wrapping the three pillars of state management into a single Hub, we
    simplify the Orchestrator's API. The rest of the system only needs to
    know about the Hub, which ensures that an update to the cache is
    correctly mirrored to the log stream and the database.
    """

    def __init__(self, db_config: dict, exec_ctx: ExecutionContext):
        """Initializes the Hub and its underlying state components.

        Args:
            db_config: Configuration for the ClickHouse telemetry sink.
            exec_ctx: The global execution context for workspace pathing.

        """
        self.exec_ctx = exec_ctx
        self.workspace_dir = exec_ctx.state_path
        self.workspace_dir.mkdir(parents=True, exist_ok=True)

        self.store = StateStore(exec_ctx)
        self.sink = StateSink(db_config, self.workspace_dir)
        self.source = StateSource(self.store, exec_ctx)

        LOG.info("StateHub initialized", workspace=str(self.workspace_dir))

    def add_task(self, task_ref: TaskRef) -> None:
        """Registers a new task in both the cache and the log stream.

        Args:
            task_ref: Routing and identity handle for the task.

        Decision: Dual Emission.
        We emit to both the Store and the Stream immediately. This
        ensures that the 'Tick' has an in-memory view for dispatching,
        while the database gets an audit record of the task's
        initial creation.
        """
        run_id = task_ref.identity.run_id
        LOG.debug("Adding task to state tracking", run_id=run_id)

        self.store.add(task_ref)
        record = self.store.get(run_id)
        if record:
            self.sink.append(record)
            LOG.debug("Task added to sink buffer", run_id=run_id)

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
        LOG.debug("Updating task state", run_id=run_id, updates=str(updates)[:200])

        if self.store.update(run_id, updates):
            record = self.store.get(run_id)
            if record:
                self.sink.append(record)
                LOG.debug("Task update appended to buffer", run_id=run_id)
        else:
            LOG.warning("Task update failed", run_id=run_id)

    def remove_task(self, run_id: str) -> None:
        """Evicts a task from the in-memory registry.

        Args:
            run_id: The ID to remove.
        """
        self.store.remove(run_id)

    def sync_manifest(self, folder_path: Path | str, deep_sync: bool = False) -> None:
        """Synchronizes a physical manifest file back into the state engine.

        The manifest file is used as canonical log for the task.
        Hence, saving ir to database for audit purposes

        Args:
            folder_path: Path to the task's workspace folder.
            deep_sync: If True, includes the full manifest JSON in the update.
        """
        if isinstance(folder_path, str) and not Path(folder_path).exists():
            resolved = find_path(self.exec_ctx.workspace_dir, folder_path)
            if not resolved:
                LOG.warning(
                    "Could not resolve path for manifest sync", path=folder_path
                )
                return
            folder_path = resolved

        self.source.sync_folder(Path(folder_path), deep_sync)

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
        self.sink.send()

    def close(self) -> None:
        """Gracefully shuts down the Hub and closes active streams."""
        LOG.info("Closing StateHub")
        self.sink.close()
