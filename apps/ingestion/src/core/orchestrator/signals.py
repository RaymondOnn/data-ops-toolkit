import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from loguru import logger

from apps.ingestion.src.core.models.task.status import ExecutionStatus
from apps.ingestion.src.utils.common import find_path

if TYPE_CHECKING:
    from core.orchestrator.manager import TaskManger

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
        engine: "TaskManger",
        exec_ctx: "ExecutionContext",
    ):
        self.state_store = state_store
        self.engine = engine
        self.exec_ctx = exec_ctx
        self._commands: dict[str, Callable] = {}

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
                LOG.debug("Processing task signal", run_id=run_id, type=ext)

                # --- THE GLOBAL STATE SYNC LOGIC ---

                # Find where the task folder is (active/, HOLD/, or FAILED/)
                # find_path looks for the directory name matches run_id
                folder_path = find_path(self.exec_ctx.workspace_dir, run_id)

                if not folder_path:
                    LOG.warning(
                        "Signal received but task folder not found", run_id=run_id
                    )
                    file.unlink()
                    continue

                if ext == ".fail":
                    # TRIGGER: Deep sync on failure to capture error message and traceback
                    LOG.error("Syncing terminal failure to StateStore", run_id=run_id)
                    self.state_store.sync_from_folder(folder_path)

                elif ext == ".done":
                    # TRIGGER: Success sync
                    LOG.success("Syncing task completion to StateStore", run_id=run_id)
                    self.state_store.sync_from_folder(folder_path)

                elif ext == ".retry":
                    # TRIGGER: Status update for intermediate retry
                    self.state_store.update_run(
                        run_id, {"status": ExecutionStatus.RETRY}
                    )

                elif ext == ".sync":
                    # TRIGGER: Basic heartbeat/progress update
                    self.state_store.update_run(run_id, {"last_active": time.time()})

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
