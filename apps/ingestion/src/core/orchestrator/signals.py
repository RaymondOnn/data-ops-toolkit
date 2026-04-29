from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.common import find_path
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.contexts import ExecutionContext
    from apps.ingestion.src.core.orchestrator.state import StateStore


LOG = logger


class SignalProcessor:
    """
    The 'Traffic Controller' for filesystem-based events.
    It translates zero-byte signal files into Global State Store updates.
    """

    def __init__(
        self,
        state_store: "StateStore",
        exec_ctx: "ExecutionContext",
    ):
        self.state_store = state_store
        self.exec_ctx = exec_ctx
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
            self.state_store.sync_from_folder(folder_path)
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
        """Handles a .sync signal, updating the last active timestamp."""
        self.state_store.update_run(run_id, {})

    def register_command(self, name: str, handler: Callable):
        """Registers handlers for .cmd files."""
        self._commands[name] = handler

    def _process_worker_signals(self, filter_run_ids: set[str] | None = None):
        """
        Scans the signals/ directory and synchronizes filesystem state to the DB.
        This is the 'Global State Sync' trigger point.
        """
        signal_path = self.exec_ctx.signal_path
        if not signal_path.exists():
            return

        # Scan for all signal types: .sync, .done, .fail, .retry, .cmd
        for file in signal_path.iterdir():
            if not file.is_file():
                continue

            # 1. Handle Commands (.cmd) - Administrative triggers
            if file.suffix == ".cmd":
                if handler := self._commands.get(file.name):
                    LOG.info("Executing signal command", command=file.name)
                    handler()
                file.unlink()
                continue

            # 2. Parse Task Signals ({job}:{dataset}:{date}:{run_id}.{ext})
            try:
                # Use the context's parser to extract metadata from filename
                _, _, _, run_id = self.exec_ctx.parse_identifier(file.stem)

                # If we are in 'Synchronous' mode, only process signals for specific runs
                if filter_run_ids and run_id not in filter_run_ids:
                    continue

                ext = file.suffix
                LOG.debug("Processing task signal", run_id=run_id, signal_type=ext)

                # --- THE GLOBAL STATE SYNC LOGIC ---

                # Find where the task folder is (active/, HOLD/, or FAILED/)
                # find_path looks for the directory name matches run_id
                folder_path = find_path(self.exec_ctx.workspace_dir, run_id)

                if handler := self._signal_handlers.get(ext):
                    handler(run_id, folder_path)
                else:
                    LOG.warning(
                        "Unknown signal file extension", filename=file.name, ext=ext
                    )

            except Exception:
                LOG.exception("Failed to process signal file", filename=file.name)
                # We don't unlink on error to allow for a retry in the next loop
            else:
                # 3. Cleanup: Always remove the signal file after it is processed
                file.unlink()

    def _check_for_manual_commands(self) -> None:
        """
        Checks for 'Command Files' dropped by CLI users/scripts.
        This acts as our inter-process communication (IPC).
        """
        cmd_dir = self.exec_ctx.signal_path

        for cmd_file, callback in self._commands.items():
            cmd_path = cmd_dir / cmd_file
            if cmd_path.exists():
                LOG.info("Manual command received", command=cmd_file)
                try:
                    # 1. Execute the command
                    callback()
                    # 2. 'Eat' the command file so it doesn't run again next tick
                    cmd_path.unlink()
                except OSError as e:
                    LOG.error(
                        "Failed to cleanup manual command file", cmd=cmd_file, error=e
                    )
                except Exception:
                    LOG.exception(
                        "Unexpected error executing manual command", cmd=cmd_file
                    )
