from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import msgspec
from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.contexts import ExecutionContext

    from .janitor import DaemonJanitor
    from .state import DaemonStateStore

LOG = logger

# Define command priorities: Lower number = Higher priority
COMMAND_PRIORITIES: dict[str, int] = {
    "STOP": 0,
    "CANCEL_RUN": 0,
    "RELOAD_CONFIG": 1,
    "RECOVER_ALL": 2,
    "RESUME": 2,
    "ADD": 3,
}


class CommandProcessor:
    """
    Processes filesystem-based command signals for the Daemon.

    Scans a designated signal directory for `.cmd` files, parses their content,
    and dispatches them to registered handlers based on priority."""

    def __init__(
        self,
        exec_ctx: "ExecutionContext",
        janitor: "DaemonJanitor",
        state_store: "DaemonStateStore",
    ):
        self.exec_ctx = exec_ctx
        """The execution context for the daemon."""
        self.janitor = janitor
        """The daemon janitor for recovery operations."""
        self.state_store = state_store
        """The daemon state store for managing job records."""

        # Registry mapping command prefixes to handlers
        self._handlers: dict[str, Callable[[Any], None]] = {
            "RECOVER_ALL": lambda _: self.janitor.recover_failed_tasks(),
            "CANCEL_RUN": self._handle_cancel_run,
            "RELOAD_CONFIG": lambda _: LOG.info(
                "Config reload logic not yet implemented"
            ),
        }

    def register_handler(
        self, command_name: str, handler: "Callable[[Any], None]"
    ) -> None:
        """Registers a new callback for a specific .cmd filename type.

        Args:
            command_name: The base name of the command (e.g., "STOP", "RESUME").
            handler: The callable function to execute when the command is found.
        """
        key = command_name.upper().replace(".CMD", "")
        self._handlers[key] = handler
        LOG.debug("Registered custom command handler", command=key)

    def process_commands(self) -> list[tuple[Callable[[Any], None], Any]]:
        """Scans the signal directory for `.cmd` files and collects actions.

        Commands are sorted by priority (defined in `COMMAND_PRIORITIES`) and
        then by file modification time. Each command file is deleted after
        processing.

        Returns:
            list[tuple[Callable[[Any], None], Any]]: A list of tuples, where
                each tuple contains a handler function and its associated payload.
        """
        cmd_dir = self.exec_ctx.signal_path
        if not cmd_dir.exists():
            return []

        command_queue = []

        def get_base_key(stem: str) -> str:
            """
            Extracts the base command name from a file stem.

            This ignores unique IDs or arguments appended to the command
            (e.g., "ADD_123" -> "ADD").

            Args:
                stem: The stem of the command file (e.g., "ADHOC_RUN_123").

            Returns:
                str: The base command name.
            """
            upper_stem = stem.upper()

            # Explicitly typing this as list[str] prevents the 'Sized' inference issue
            sorted_keys: list[str] = sorted(
                COMMAND_PRIORITIES.keys(), key=len, reverse=True
            )

            return next(
                (k for k in sorted_keys if upper_stem.startswith(k)), upper_stem
            )

        # Hybrid Priority-FIFO: Sort by Priority Level, then by File Modification Time
        cmd_files = sorted(
            cmd_dir.glob("*.cmd"),
            key=lambda p: (
                COMMAND_PRIORITIES.get(get_base_key(p.stem), 99),
                p.stat().st_mtime,
            ),
        )

        for cmd_file_path in cmd_files:
            # The filename is the command (e.g., ADHOC_RUN.cmd -> ADHOC_RUN)
            cmd_key = cmd_file_path.stem.upper()
            base_key = get_base_key(cmd_key)

            handler = self._handlers.get(base_key)
            if not handler:
                LOG.warning("Unknown command file detected", command=cmd_key)
                cmd_file_path.unlink()
                continue

            try:
                # Load JSON payload from file
                payload = None
                if cmd_file_path.stat().st_size > 0:
                    content = cmd_file_path.read_bytes()
                    try:
                        payload = msgspec.json.decode(content)
                    except msgspec.DecodeError:
                        # Fallback for plain-text signals (e.g. legacy STOP.cmd content)
                        payload = content.decode("utf-8").strip()

                LOG.debug("Queued signal command", command=cmd_key)
                command_queue.append((handler, payload))
            except Exception:
                LOG.exception("Error parsing command file", command=cmd_key)
            finally:
                cmd_file_path.unlink()

        return command_queue

    def _handle_cancel_run(self, data: Any) -> None:
        """Handles the `CANCEL_RUN` command to stop a specific run.

        This method logs the cancellation and would typically interact with
        the `TaskManager` and `Compute` engine to stop the run and reclaim
        resources.

        Args:
            data: The payload associated with the command, expected to contain a `run_id`.
        """
        run_id = data.get("run_id") if isinstance(data, dict) else data
        if not run_id:
            LOG.error("CANCEL_RUN requires a run_id (e.g., CANCEL_RUN:run123.cmd)")
            return

        LOG.warning("Manually cancelling run", run_id=run_id)
        # 1. Pop from Orchestrator Queues (Hot Cache)
        # Implementation would search TaskManager.cache for the run_id

        # 2. Reclaim Ray Resources
        # Implementation would trigger Compute.reclaim_resources

        # 3. Mark as CANCELLED in StateStore
        self.state_store.store.update_run(run_id, {"JOB_STATUS": "CANCELLED"})
