import threading
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from apps.ingestion.src.core.models.task import Task
from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.common import find_path
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.contexts import ExecutionContext
    from apps.ingestion.src.core.orchestrator.janitor import Janitor
    from apps.ingestion.src.core.orchestrator.state import StateStore


LOG = logger


class InternalEventBus:
    def __init__(self):
        self._subscribers = []
        self._change_event = threading.Event()

    def notify(self):
        """Wake up everyone waiting for a change."""
        self._change_event.set()

    def wait_for_change(self, timeout=None):
        """Block until someone calls notify()."""
        signaled = self._change_event.wait(timeout=timeout)
        self._change_event.clear()
        return signaled


class SignalProcessor(InternalEventBus):
    """
    The 'Traffic Controller' for filesystem-based events.
    It translates zero-byte signal files into Global State Store updates.
    """

    def __init__(
        self,
        state_store: "StateStore",
        exec_ctx: "ExecutionContext",
        janitor: "Janitor | None" = None,
    ):
        super().__init__()
        self.state_store = state_store
        self.exec_ctx = exec_ctx
        self.janitor = janitor
        self._commands: dict[str, Callable] = {}
        self._signal_handlers: dict[str, Callable[[str, Path | None], None]] = {
            ".fail": self._handle_fail_signal,
            ".done": self._handle_done_signal,
            ".retry": self._handle_retry_signal,
            ".sync": self._handle_sync_signal,
        }

    def _handle_fail_signal(self, run_id: str, folder_path: Path | None) -> None:
        """Handles a .fail signal, performing a deep sync to capture error details."""
        LOG.error("Syncing terminal failure to StateStore", run_id=run_id)
        if folder_path:
            self.state_store.sync_from_folder(folder_path)
        else:
            LOG.warning(
                "Failed signal for non-existent folder, emitting synthetic expiry",
                run_id=run_id,
            )
            # If folder is gone, emit a synthetic expiry to mark it as failed in DB
            self.state_store.emit_expiry(
                run=self.state_store.active_registry.get(run_id),  # type: ignore
                context=None,
                reason=f"Failed signal received, but folder not found for {run_id}",
            )

    def _handle_done_signal(self, run_id: str, folder_path: Path | None) -> None:
        """Handles a .done signal, performing a deep sync for completion."""
        LOG.success("Syncing task completion to StateStore", run_id=run_id)
        if folder_path:
            # 1. Final Deep Sync to DB
            self.state_store.sync_from_folder(folder_path, deep_sync=True)

            # 2. Trigger Janitor Purge
            if self.janitor:
                # Rehydrate task to perform identity-aware cleanup
                task = Task.from_folder(folder_path, self.exec_ctx)
                self.janitor.cleanup_task(task)
        else:
            LOG.warning(
                "Done signal for non-existent folder, emitting synthetic expiry",
                run_id=run_id,
            )
            # If folder is gone, emit a synthetic expiry to mark it as done in DB
            self.state_store.emit_expiry(
                run=self.state_store.active_registry.get(run_id),  # type: ignore
                context=None,
                reason=f"Done signal received, but folder not found for {run_id}",
            )

    def _handle_retry_signal(
        self, run_id: str, folder_path: Path | None = None
    ) -> None:
        """Handles a .retry signal, updating the job status."""
        self.state_store.update_run(run_id, {"JOB_STATUS": ExecutionStatus.RETRY.value})

    def _handle_sync_signal(self, run_id: str, folder_path: Path | None = None) -> None:
        """Handles a .sync signal, performing a shallow sync to update metrics."""
        if folder_path:
            self.state_store.sync_from_folder(folder_path, deep_sync=False)
        else:
            # Fallback to a simple heartbeat if the folder isn't resolved
            self.state_store.update_run(run_id, {})

    def register_command(self, name: str, handler: Callable):
        """Registers handlers for .cmd files."""
        self._commands[name] = handler

    def _process_worker_signals(self, filter_run_ids: set[str] | None = None) -> bool:
        """
        Scans the signals/ directory and synchronizes filesystem state to the DB.
        Returns True if any signals were processed, triggering a notification.
        """
        signal_path = self.exec_ctx.signal_path
        if not signal_path.exists():
            return False

        state_changed = False

        for file in signal_path.iterdir():
            if not file.is_file():
                continue

            # 1. Handle Commands (.cmd)
            if file.suffix == ".cmd":
                if handler := self._commands.get(file.name):
                    LOG.info("Executing signal command", command=file.name)
                    handler()
                    state_changed = True  # Commands likely change system state
                file.unlink()
                continue

            # 2. Parse Task Signals
            try:
                _, _, _, run_id = self.exec_ctx.parse_identifier(file.stem)

                if filter_run_ids and run_id not in filter_run_ids:
                    continue

                ext = file.suffix
                folder_path = find_path(self.exec_ctx.workspace_dir, run_id)

                if handler := self._signal_handlers.get(ext):
                    handler(run_id, folder_path)
                    state_changed = True  # A task stage finished, failed, or synced
                else:
                    LOG.warning("Unknown signal extension", ext=ext)

            except Exception:
                LOG.exception("Failed to process signal file", filename=file.name)
            else:
                file.unlink()

        # If anything happened, wake up the Orchestrator observers
        if state_changed:
            self.notify()

        return state_changed

    def _check_for_manual_commands(self) -> None:
        """Checks for manual IPC command files and notifies the bus on success."""
        cmd_dir = self.exec_ctx.signal_path
        executed_any = False

        for cmd_file, callback in self._commands.items():
            cmd_path = cmd_dir / cmd_file
            if cmd_path.exists():
                LOG.info("Manual command received", command=cmd_file)
                try:
                    callback()
                    cmd_path.unlink()
                    executed_any = True
                except Exception:
                    LOG.exception("Error executing manual command", cmd=cmd_file)

        if executed_any:
            self.notify()
