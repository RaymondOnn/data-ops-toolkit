from pathlib import Path
from typing import Callable

import structlog

from src.core.orchestrator.engine import IngestionEngine
from src.core.orchestrator.state import StateStore
from src.utils.constants import JOB_STEPS_BASE_DIR

LOG = structlog.getLogger(__name__)


def resolve_current_path(job_id: str, run_id: str, status: str, step: str) -> Path:
    """
    Returns the physical path of a job based on its Approach 1 status.
    """
    from src.core.models.job import JobStatus

    base = Path(JOB_STEPS_BASE_DIR)

    # Branch based on Machine State
    if status in [JobStatus.FAILED]:
        return base / "FAILED" / job_id / run_id
    elif status in (JobStatus.BLOCKED or JobStatus.DEFERRED,):
        return base / "HOLD" / job_id / run_id
    else:
        # Standard flow uses the lowercase step name as the folder
        return base / step.lower() / job_id / run_id


class SignalProcessor:
    def __init__(self, state_store: StateStore, engine: IngestionEngine) -> None:
        self.state_store = state_store
        self.engine = engine
        self._command_registry: dict[str, Callable[[], None]] = {}

    def register_command(self, cmd_name: str, callback: Callable[[], None]) -> None:
        """Registers a method to be called when a .cmd file appears."""
        self._command_registry[cmd_name] = callback

    def _process_worker_signals(self) -> None:
        """
        Scans the flat signals directory for any {run_id}.signal files.

        The worker writes the manifest.json first, then "drops" the signal file.
        This prevents the Orchestrator from reading a manifest that the worker
        is still writing.
        """
        signal_dir = JOB_STEPS_BASE_DIR / "signals"
        if not signal_dir.exists():
            return

        # Define our signals and whether they require a deep manifest sync
        # .sync = Light heartbeat | .done = Final deep audit
        signals = {"*.sync": False, "*.done": True}

        # iterdir() returns a generator, which is memory efficient
        # This glob automatically ignores files starting with "."
        for pattern, is_deep_sync in signals.items():
            for crumb in signal_dir.glob("[!.]*.sync"):
                try:
                    # 1. Parse metadata from filename
                    # Example: 20240101-abc.transform.3.sync
                    # Metadata is now primarily in the manifest;
                    # filename is just a pointer to the run_id
                    run_id = crumb.stem.split(".")[0]
                    # 2. Find the folder path from DB
                    run_record = self.state_store.get_run(run_id)
                    if not run_record:
                        LOG.error("Signal received for unknown run", run_id=run_id)
                        continue

                    # Resolve the path using our new utility
                    physical_path = resolve_current_path(
                        job_id=run_record["job_id"],
                        run_id=run_id,
                        status=run_record["status"],
                        step=run_record["step"],
                    )

                    # 3. Sync manifest -> DB
                    self.state_store.sync_from_folder(Path(physical_path), deep_sync=is_deep_sync)

                    # 4. 'Eat' the breadcrumb
                    crumb.unlink(missing_ok=True)
                    LOG.debug("Signal processed", run_id=run_id)

                except Exception as e:
                    LOG.error(f"Failed to process breadcrumb {crumb.name}: {e}")
                    LOG.debug("Signal processed", run_id=run_id)

    def _check_for_manual_commands(self) -> None:
        """
        Checks for 'Command Files' dropped by CLI users/scripts.
        This acts as our inter-process communication (IPC).
        """
        cmd_dir = Path(JOB_STEPS_BASE_DIR) / "signals"

        for cmd_file, callback in self._command_registry.items():
            cmd_path = cmd_dir / cmd_file
            if cmd_path.exists():
                LOG.info("Manual command received", command=cmd_file)
                try:
                    # 1. Execute the command
                    callback()
                    # 2. 'Eat' the command file so it doesn't run again next tick
                    cmd_path.unlink()
                except Exception as e:
                    LOG.error("Failed to execute manual command", cmd=cmd_file, error=e)
