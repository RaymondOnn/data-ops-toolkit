from collections.abc import Callable
from pathlib import Path

import structlog

from apps.ingestion.src.core.contexts import ExecutionContext
from apps.ingestion.src.core.orchestrator.engine import IngestionEngine
from apps.ingestion.src.core.orchestrator.state import StateStore

LOG = structlog.getLogger(__name__)


class SignalProcessor:
    def __init__(
        self,
        state_store: StateStore,
        engine: IngestionEngine,
        exec_ctx: ExecutionContext,
    ) -> None:
        self.state_store = state_store
        self.engine = engine
        self.exec_ctx = exec_ctx
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
        signal_dir = self.exec_ctx.signal_path
        if not signal_dir.exists():
            return

        # Define our signals and whether they require a deep manifest sync
        # .sync = Light heartbeat | .done = Final deep audit
        signals = {"*.sync": False, "*.done": True}

        # iterdir() returns a generator, which is memory efficient
        # This glob automatically ignores files starting with "."
        for pattern, is_deep_sync in signals.items():
            for signal in signal_dir.glob(pattern):
                run_id = None
                try:
                    # 1. Parse metadata from filename
                    # Example: 20240101-abc.transform.3.sync
                    run_id = signal.stem.split(".")[0]
                    record = self.state_store.active_records.get(run_id)

                    # If not in cache, leave it for the next iteration.
                    # This handles the gap between worker start and DB flush.
                    if not record:
                        LOG.debug("Signal for unknown run_id, deferring", run_id=run_id)
                        continue

                    # Resolve the path using our new utility
                    job_dir = (
                        self.exec_ctx.workspace_dir
                        / "active"
                        / f"{record['JOB_ID']}:{record['DATASET_ID']}_{record['RUN_DATE']}"
                        / record["RUN_ID"]
                    )

                    # 3. Sync manifest -> DB
                    self.state_store.sync_from_folder(
                        Path(job_dir), deep_sync=is_deep_sync
                    )

                    # 4. Remove the signal
                    signal.unlink(missing_ok=True)
                    LOG.debug("Signal processed", run_id=run_id)

                except (OSError, ValueError) as e:
                    LOG.error(
                        "Failed to process signal", signal=signal.name, error=str(e)
                    )
                except Exception:
                    LOG.exception(
                        "Unexpected error processing signal", signal=signal.name
                    )

    def _check_for_manual_commands(self) -> None:
        """
        Checks for 'Command Files' dropped by CLI users/scripts.
        This acts as our inter-process communication (IPC).
        """
        cmd_dir = self.exec_ctx.signal_path

        for cmd_file, callback in self._command_registry.items():
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
