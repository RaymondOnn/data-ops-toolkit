from collections.abc import Callable
from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from apps.ingestion.src.core.contexts import ExecutionContext
    from apps.ingestion.src.core.orchestrator.janitor import Janitor
    from apps.ingestion.src.core.orchestrator.manager import TaskManager
    from apps.ingestion.src.core.orchestrator.state import StateStore

LOG = logger


class CommandProcessor:
    def __init__(
        self,
        exec_ctx: "ExecutionContext",
        janitor: "Janitor",
        task_manager: "TaskManager",
        state_store: "StateStore",
    ):
        self.exec_ctx = exec_ctx
        self.janitor = janitor
        self.task_manager = task_manager
        self.state_store = state_store

        # Registry mapping command prefixes to handlers
        self._handlers: dict[str, Callable[[str | None], None]] = {
            "RECOVER_ALL": lambda _: self.janitor.recover_failed_tasks(),
            "CANCEL_RUN": self._handle_cancel_run,
            "RELOAD_CONFIG": lambda _: LOG.info(
                "Config reload logic not yet implemented"
            ),
        }

    def process_commands(self) -> bool:
        """
        Scans signals/ for .cmd files and dispatches to registered handlers.
        Returns True if any commands were processed.
        """
        cmd_dir = self.exec_ctx.signal_path
        if not cmd_dir.exists():
            return False

        processed_any = False
        for cmd_file_path in cmd_dir.glob("*.cmd"):
            # Support format: COMMAND_NAME:ARGUMENT.cmd
            full_stem = cmd_file_path.stem  # e.g., "CANCEL_RUN:20260505-1234"
            parts = full_stem.split(":", 1)
            cmd_key = parts[0].upper()
            argument = parts[1] if len(parts) > 1 else None

            handler = self._handlers.get(cmd_key)
            if not handler:
                LOG.warning("Unknown command file detected", command=cmd_key)
                cmd_file_path.unlink()
                continue

            LOG.info("Executing signal command", command=cmd_key, arg=argument)
            try:
                handler(argument)
                processed_any = True
            except Exception:
                LOG.exception("Error executing command", command=cmd_key)
            finally:
                cmd_file_path.unlink()

        return processed_any

    def _handle_cancel_run(self, run_id: str | None) -> None:
        """Logic to stop a specific run and evict it from queues."""
        if not run_id:
            LOG.error("CANCEL_RUN requires a run_id (e.g., CANCEL_RUN:run123.cmd)")
            return

        LOG.warning("Manually cancelling run", run_id=run_id)
        # 1. Pop from Orchestrator Queues (Hot Cache)
        # Implementation would search TaskManager.cache for the run_id

        # 2. Reclaim Ray Resources
        # Implementation would trigger Compute.reclaim_resources

        # 3. Mark as CANCELLED in StateStore
        self.state_store.update_run(run_id, {"JOB_STATUS": "CANCELLED"})
